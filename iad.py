# 3rd Party Modules
from internetarchive import ArchiveSession, File
from hurry.filesize import size
import yaml
import requests

# Part of Standard Library
import os
import time
import hashlib
import sys
import copy

# Used for Threading
import threading
from queue import Queue

# Logging Modules
import logging
from logging.config import dictConfig
logger = logging.getLogger(__name__)


class InternetArchiveDownloader:
    """
    I.A.D
    A simple wrapper to expand on the internet archive package. Adds reliability, resuming downloads, bulk downloads, etc
    """
    def __init__(self, config_file):
        """
        Initializes an instance of the downloader
        :param config_file: path to the config file for I.A.D
        """

        self.ia_ini = None
        self.threads = None
        self.identifiers = None
        self.file_extension_exclusion = None
        self.download_path = None
        self.max_retries = None
        self.ia_ini_path = None
        self.skip_duplicate_files = None
        self.file_integrity_type = None
        self.percentage_sleep = None

        self.download_left = {}
        self.session = None
        self.queue = None
        self.file_count = 0

        self.apply_config_file(config_file)

    def apply_config_file(self, config_file):
        """
        Reads the yaml of the given config file and assigns values
        :param config_file: Path to config file
        """

        data = None
        with open(config_file, 'r') as stream:
            try:
                data = yaml.load(stream)
            except yaml.YAMLError as exc:
                if hasattr(exc, 'problem_mark'):
                    mark = exc.problem_mark
                    print("Error position: (%s:%s)" % (mark.line + 1, mark.column + 1))
                exit(1)

        # Set the passed in vars to the class vars
        try:
            iad_config = data["iad"]
            self.threads = iad_config["threads"]
            self.identifiers = iad_config["identifiers"]
            self.file_extension_exclusion = iad_config["file_extension_exclusion"]
            self.download_path = iad_config["download_path"]
            self.max_retries = iad_config["max_retries"]
            self.ia_ini_path = iad_config["ia_ini_path"]
            self.skip_duplicate_files = iad_config["skip_duplicate_files"]
            self.file_integrity_type = iad_config["file_integrity_type"]
            self.percentage_sleep = iad_config["percentage_sleep"]
            logging.config.dictConfig(data["logging"])
        except KeyError as error:
            print(error)
            raise

        # Initialize Queue
        self.queue = Queue()

    def _create_session(self):
        """
        Creates an internet archive session using the credentials/cookies in ia.ini file
        :return:
        """

        self.session = ArchiveSession(config_file=self.ia_ini)
        logger.info("Successfully create session.")

    def _get_item(self, identifier):
        """
        Gets a item object that is used to grab files and other attributes
        :param identifier: Internet Archive identifier string
        :return: Returns a item object or None if the identifier does not exist
        """

        item = self.session.get_item(identifier)
        if item.exists:
            return item
        else:
            logger.error(identifier + " could not be found!")
            return None

    def _filter_files(self, item):
        """
        Filters files out that are already downloaded and verified, and if their file extension is on the exclusion list
        :param item: Internet Archive Item Object
        :return: A filtered Item Object
        """

        # Making a copy of the list of files because you can not remove a element while iterating over the list
        files = copy.deepcopy(item.files)

        # Declares a var to hold filtered number of files for logging
        filtered_count = 0

        # Loops through each file
        for file in files:
            # Grabs the file name and extension of each file
            name, extension = os.path.splitext(file["name"])

            # Checks if file is in the folder already
            if file["name"] in os.listdir(self.download_path + item.identifier + "/"):
                # Checking if the size/hash matches
                if self._check_file_integrity(self.download_path + item.identifier + "/" + file["name"], file, self.file_integrity_type):
                    item.files.remove(file)
                    filtered_count = filtered_count + 1
                    logger.debug("Removed " + file["name"] + " because it was detected in the folder path.")
                else:
                    logger.error("Detected file integrity issue with " + file["name"])
                    os.remove(self.download_path + item.identifier + "/" + file["name"])
                    logger.info("Removed " + file["name"] + " for file integrity issues, forcing fresh download!")
            # Checks if the file extension is on the exclusion list
            elif extension in self.file_extension_exclusion:
                item.files.remove(file)
                filtered_count = filtered_count + 1
                logger.debug("Removed " + file["name"] + " because it's file extension was excluded.")

        logger.info("Removed " + str(filtered_count) + " files from download list")

        # Need to update stats of filtered item
        return self._update_item_stats(item)

    @staticmethod
    def _update_item_stats(item):
        """
        Updates needed Stats of an Item
        :param item: Internet Archive Item Object
        :return: Updated Item Oject
        """

        # Updates the File Count
        item.files_count = len(item.files)
        logger.debug(item.identifier + " should be at " + str(item.files_count) + " main files")

        # Calculates new size of all the files in a given item
        item_size = 0
        for file in item.files:
            item_size = item_size + int(file["size"])

        item.item_size = item_size
        logger.debug(item.identifier + " should be at " + str(item.item_size) + " size")

        logger.info("Updated stats of " + item.identifier)
        return item

    def _populate_download_left_dict(self, items):
        """
        Creates a download left dictionary
        :param items: List of Internet Archive Item Objects
        """
        for item in items:
            self.download_left[item.identifier] = item.item_size

    def _add_to_files_to_queue(self, items):
        """
        Adds files to the queue to be processed
        :param items: List of Internet Archive Item Objects
        """
        for item in items:
            for file in item.files:
                self.queue.put((item, file))
                self.file_count = self.file_count + 1

        logging.info("Added " + str(self.file_count) + " files to the queue")

    def _download_file(self, item, file):
        """
        Downloads a given file and logs stats
        :param item: Internet Archive Item Object
        :param file: Internet Archive File Object
        """
        logger.info("File being Downloaded: " + file["name"] + "\n" +
                    file["name"] + " Size: " + size(int(file["size"])))

        start_time = time.perf_counter()
        total_retries = 0
        max_retries = self.max_retries
        file_path = self.download_path + item.identifier + "/" + file["name"]

        # Spawns monitor thread to print out percentages of download
        monitor_thread = threading.Thread(target=self._monitor_download,
                                          kwargs={"file_path": file_path, "total_size": file["size"]})
        monitor_thread.start()

        # Loop keep trying to download file
        while max_retries != 0 or total_retries == 0:
            try:
                File(item, file["name"]).download(file_path=file_path)
                break
            except requests.exceptions.ConnectionError as error:
                logger.error(error)
                total_retries = total_retries + 1
                max_retries = max_retries - 1
                logger.debug("Retrying Download for " + file["name"] + ".....")

        end_time = time.perf_counter()
        self.download_left[item.identifier] = self.download_left[item.identifier] - int(file["size"])

        logger.info(file["name"] + " successfully downloaded!\n" +
                    "Download left for " + item.identifier + ": " + size(self.download_left[item.identifier]) + "\n"
                    "Files Left to go: " + str(self.queue.qsize()))

        logger.debug(file["name"] + " Download Stats:\n" +
                     "Total Retries: " + str(total_retries) + "\n" +
                     "Time: " + str(end_time - start_time) + "secs\n"
                     "Average Speed: " + size(int(file["size"]) / (end_time - start_time)) + "/s")

    def _check_identifier_folder(self, identifier):
        """
        Check if folder has been made for the identifier
        :param identifier: Folder Name
        """
        if os.path.isdir(self.download_path + identifier) is not True:
            os.mkdir(self.download_path + identifier)
            logger.info("Created " + self.download_path + identifier + " folder!")

    @staticmethod
    def _hash_bytestr_iter(bytesiter, hasher, ashexstr=False):
        """
        Takes chucks of files and hashes them all together
        :param bytesiter: Block of file data
        :param hasher: Method to hash file by
        :param ashexstr: ???
        :return: Returns a hash of the file
        """
        for block in bytesiter:
            hasher.update(block)
        return hasher.hexdigest() if ashexstr else hasher.digest()

    @staticmethod
    def _file_as_blockiter(afile, blocksize=65536):
        """
        Takes a file a slowly breaks it up into chucks so we can hash it. Helps on memory by breaking into chunks
        :param afile: The file to be hashes
        :param blocksize: ???
        """
        with afile:
            block = afile.read(blocksize)
            while len(block) > 0:
                yield block
                block = afile.read(blocksize)

    @staticmethod
    def _check_file_integrity(file_path, file, method="size"):
        """
        Checks if downloaded file is corrupt
        :param file_path: Path to downloaded file
        :param file: Internet Archive File Object
        :param method: String of the method you want to hash: md5, sha1 or size
        :return: True if file is not corrupt
        """
        if method.lower() == "size":
            if os.path.getsize(file_path) == int(file["size"]):
                return True
            else:
                return False
        elif method.lower() == "md5":
            md5_hash = InternetArchiveDownloader._hash_bytestr_iter(InternetArchiveDownloader._file_as_blockiter(open(file_path, 'rb')), hashlib.md5())
            logger.debug("IA File MD5 Hash: " + file["md5"] + " File MD5 Hash: " + md5_hash)
            if md5_hash == file["md5"]:
                return True
            else:
                return False
        elif method.lower() == "sha1":
            sha1_hash = InternetArchiveDownloader._hash_bytestr_iter(InternetArchiveDownloader._file_as_blockiter(open(file_path, 'rb')), hashlib.sha1())
            logger.debug("IA File SHA1 Hash: " + file["sha1"] + " File SHA1 Hash: " + sha1_hash)
            if sha1_hash == file["sha1"]:
                return True
            else:
                return False

    def _worker(self):
        """"
        Worker thread
        """
        while True:
            # Grabs a item and file from the queue
            item_file = self.queue.get()

            # Processes the domain
            self._download_file(item_file[0], item_file[1])

            # Marks Task as done
            self.queue.task_done()

    def _create_thread_pool(self):
        """
        Creates the number of threads requested
        """
        for i in range(self.threads):
            self._create_thread()

    def _create_thread(self):
        """
        Creates an individual thread
        """
        # Creates a thread with target function _worker
        t = threading.Thread(target=self._worker)

        # thread dies when main thread (only non-daemon thread) exits.
        t.daemon = True

        # Activates the thread
        t.start()

    def _monitor_download(self, file_path=None, total_size=None):
        total_size = int(total_size)
        current_size = 0
        try:
            current_size = os.path.getsize(file_path)
        except FileNotFoundError:
            logger.error(file_path + " not found yet!")

        while current_size != total_size:
            logger.info("TOTAL SIZE: " + total_size + " Current Size: " + str(current_size))
            logger.info("Download Progress(" + size(total_size) + "): " +
                        str(self.percentage(current_size, total_size)) + "%")
            time.sleep(self.percentage_sleep)
            try:
                current_size = os.path.getsize(file_path)
            except FileNotFoundError:
                logger.error(file_path + " not found yet!")

        logger.debug("Killing Thread?")
        sys.exit(0)

    @staticmethod
    def percentage(part, whole):
        return 100 * float(part) / float(whole)

    def run(self):
        # Creates timer to see how long the program takes
        start_time = time.perf_counter()

        # Creates an Session to download files with
        self._create_session()

        items = []
        # Loop over identifiers, filters existing items and adds them to list of items
        for identifier in self.identifiers:
            item = self._get_item(identifier)
            if item:
                self._check_identifier_folder(identifier)
                item = self._filter_files(item)
                items.append(item)

        # Creates the download left dictionary
        self._populate_download_left_dict(items)

        # Adds files to the queue to be processed
        self._add_to_files_to_queue(items)

        # Creates the needed threads
        self._create_thread_pool()
        logger.info("Created Thread pool")

        while True:
            # If queue is empty break out of loop
            if self.queue.empty():
                self.queue.join()
                break

        end_time = time.perf_counter()
        logger.info("Downloaded " + str(self.file_count) + " in " + str(end_time - start_time) + "secs")


def main(args):
    download = InternetArchiveDownloader(args[1])
    download.run()


if __name__ == "__main__":
    main(sys.argv)
