import os
import time
import hashlib
import sys
import copy
import threading
import datetime
import logging
from logging import config
from queue import Queue

from internetarchive import ArchiveSession, File
from hurry.filesize import size
import yaml
import requests

LOGGER = logging.getLogger(__name__)


class InternetArchiveDownloader:
    """
    I.A.D
    A simple wrapper to expand on the internet archive package. Adds reliability, resuming downloads, bulk downloads,
    etc
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
            except yaml.YAMLError as error:
                if hasattr(error, 'problem_mark'):
                    mark = error.problem_mark
                    print("Error position: ({line}:{column})".format(line=mark.line + 1, column=mark.column + 1))
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
            sys.exit(1)

        # Initialize Queue
        self.queue = Queue()

    def _create_session(self):
        """
        Creates an internet archive session using the credentials/cookies in ia.ini file
        :return:
        """

        self.session = ArchiveSession(config_file=self.ia_ini)
        LOGGER.info("Successfully create session.")

    def _get_item(self, identifier):
        """
        Gets a item object that is used to grab files and other attributes
        :param identifier: Internet Archive identifier string
        :return: Returns a item object or None if the identifier does not exist
        """

        item = self.session.get_item(identifier)
        if not item.exists:
            LOGGER.error("%s dsjksakdasds", identifier)
            LOGGER.error("{identifier} could not be found!", identifier=identifier)
            item = None

        return item

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
            extension = os.path.splitext(file["name"])[1]

            # Checks if file is in the folder already
            if file["name"] in os.listdir(self.download_path + item.identifier + "/"):
                # Checking if the size/hash matches
                try:
                    if self._check_file_integrity(self.download_path + item.identifier + "/" + file["name"], file,
                                                  self.file_integrity_type):

                        item.files.remove(file)
                        filtered_count = filtered_count + 1
                        LOGGER.debug("Removed {name} from download list because it was detected in the folder path.".
                                     format(name=file["name"]))
                    else:
                        LOGGER.error("Detected file integrity issue with {name}".format(name=file["name"]))
                        os.remove(self.download_path + item.identifier + "/" + file["name"])
                        LOGGER.info("Removed {name} for file integrity issues, forcing fresh download!".
                                    format(name=file["name"]))
                except ValueError as error:
                    logging.error(error)
                    sys.exit(1)
            # Checks if the file extension is on the exclusion list
            elif extension in self.file_extension_exclusion:
                item.files.remove(file)
                filtered_count = filtered_count + 1
                LOGGER.debug("Removed {name} because it's file extension was excluded.".format(name=file["name"]))

        LOGGER.info("Removed {count} files from download list".format(count=filtered_count))

        # Need to update stats of filtered item
        return self._update_item_stats(item)

    @staticmethod
    def _update_item_stats(item):
        """
        Updates needed Stats of an Item
        :param item: Internet Archive Item Object
        :return: Updated Item Object
        """

        # Updates the File Count
        item.files_count = len(item.files)
        LOGGER.debug("{identifier} should be at {count} main files".
                     format(identifier=item.identifier, count=item.files_count))

        # Calculates new size of all the files in a given item
        item_size = 0
        for file in item.files:
            item_size = item_size + int(file["size"])

        item.item_size = item_size
        LOGGER.debug("{identifier} should be at {size}".format(identifier=item.identifier, size=size(item.item_size)))

        LOGGER.info("Updated stats of {identifier}".format(identifier=item.identifier))
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

        logging.info("Added {count} files to the queue".format(count=self.file_count))

    def _download_file(self, item, file):
        """
        Downloads a given file and logs stats
        :param item: Internet Archive Item Object
        :param file: Internet Archive File Object
        """
        LOGGER.info("File being Downloaded({size}): {name}".format(size=size(int(file["size"])), name=file["name"]))

        # Start Timer
        start_time = time.perf_counter()

        # Sets up vars to keep track of retries
        total_retries = 0
        max_retries = self.max_retries

        file_path = self.download_path + item.identifier + "/" + file["name"]

        # Spawns monitor thread to print out percentages of download
        monitor_thread = threading.Thread(target=self._monitor_download,
                                          kwargs={"file_path": file_path, "total_size": file["size"],
                                                  "file_name": file["name"]})
        monitor_thread.start()

        # Loop keep trying to download file
        while max_retries != 0 or total_retries == 0:
            try:
                File(item, file["name"]).download(file_path=file_path)
                break
            except requests.exceptions.ConnectionError as error:
                LOGGER.error(error)
                total_retries = total_retries + 1
                max_retries = max_retries - 1
                LOGGER.debug("Retrying Download for {name}.....".format(name=file["name"]))

        # End Timer
        end_time = time.perf_counter()

        # Adjusting the Download Left Stats
        self.download_left[item.identifier] = self.download_left[item.identifier] - int(file["size"])

        LOGGER.info("{name} successfully downloaded!".format(name=file["name"]))
        LOGGER.info("Download left for {identifier}: {size}".
                    format(identifier=item.identifier, size=size(self.download_left[item.identifier])))
        LOGGER.info("Files Left to go: {size}".format(size=self.queue.qsize()))

        LOGGER.debug("{name} Download Stats:".format(name=file["name"]))
        LOGGER.debug("Total Retries: {retries}".format(retries=total_retries))
        LOGGER.debug("Time: {seconds}".format(seconds=datetime.timedelta(seconds=(end_time - start_time))))
        LOGGER.debug("Average Speed: {speed}/s".format(speed=(size(int(file["size"]) / (end_time - start_time)))))
        LOGGER.debug("Estimated Time left: {time}".format(time=self._get_estimated_time_left(
            self.download_left[item.identifier], (int(file["size"]) / (end_time - start_time)))))

    def _check_identifier_folder(self, identifier):
        """
        Check if folder has been made for the identifier
        :param identifier: Folder Name
        """
        if os.path.isdir(self.download_path + identifier) is not True:
            os.mkdir(self.download_path + identifier)
            LOGGER.info("Created {path}{identifier} folder!".format(path=self.download_path, identifier=identifier))

    @staticmethod
    def _hash_bytestr_iter(bytesiter, hasher, ashexstr=False):
        """
        Takes chucks of files and hashes them all together
        :param bytesiter: Block of file data
        :param hasher: Method to hash file by
        :param ashexstr: If False, function returns a hex string
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
        :param blocksize: How much each chunk of the file is read in
        """
        with afile:
            block = afile.read(blocksize)
            while block:
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
        if method.lower() != "size" and method.lower() != "md5" and method.lower() != "sha1":
            raise ValueError("Method Arg, {arg}, is not an accepted method type!".format(arg=method))

        if method.lower() == "size":
            LOGGER.debug("IA File Size: {ia_size} File Size: {os_size}".
                         format(ia_size=file["size"], os_size=os.path.getsize(file_path)))
            return bool(os.path.getsize(file_path) == int(file["size"]))
        if method.lower() == "md5":
            md5_hash = InternetArchiveDownloader._hash_bytestr_iter(
                InternetArchiveDownloader._file_as_blockiter(open(file_path, 'rb')), hashlib.md5(), True)
            LOGGER.debug("IA File MD5 Hash: {ia_hash} File MD5 Hash: {os_hash}".
                         format(ia_hash=file["md5"], os_hash=md5_hash))
            return bool(md5_hash == file["md5"])
        if method.lower() == "sha1":
            sha1_hash = InternetArchiveDownloader._hash_bytestr_iter(
                InternetArchiveDownloader._file_as_blockiter(open(file_path, 'rb')), hashlib.sha1(), True)
            LOGGER.debug("IA File SHA1 Hash: {ia_hash} File SHA1 Hash: {os_hash}".
                         format(ia_hash=file["sha1"], os_hash=sha1_hash))
            return bool(sha1_hash == file["sha1"])

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
        for _ in range(self.threads):
            self._create_thread()

    def _create_thread(self):
        """
        Creates an individual thread
        """
        # Creates a thread with target function _worker
        thread = threading.Thread(target=self._worker)

        # thread dies when main thread (only non-daemon thread) exits.
        thread.daemon = True

        # Activates the thread
        thread.start()

    def _monitor_download(self, file_path=None, total_size=None, file_name=None):
        """
        Monitors the download progress of a file
        :param file_path: Path of the file to be monitored
        :param total_size: Total Size of the File
        """

        total_size = int(total_size)
        current_size = 0

        # Wait a second or two to get the file downloading
        time.sleep(5)

        try:
            current_size = os.path.getsize(file_path)
        except FileNotFoundError:
            LOGGER.error("{file_path} not found yet!".format(file_path=file_path))

        while current_size != total_size:
            LOGGER.info("Download Progress({total_size}) for {name}: {percentage}%".
                        format(total_size=size(total_size), name=file_name,
                               percentage=self.percentage(current_size, total_size)))

            # Sleep as to not spam the log file
            time.sleep(self.percentage_sleep)
            try:
                current_size = os.path.getsize(file_path)
            except FileNotFoundError:
                LOGGER.error("{file_path} not found yet!".format(file_path=file_path))

        LOGGER.debug("Killing Thread!")
        sys.exit(0)

    @staticmethod
    def percentage(part, whole):
        """
        Simple function to generate a percentage
        :param part: Part of the number
        :param whole: Whole of the number
        :return: Percentage
        """
        return 100 * float(part) / float(whole)

    @staticmethod
    def _get_estimated_time_left(download_left, speed):
        time_left = download_left/speed
        current_time = datetime.datetime.now().timestamp()

        return datetime.datetime.fromtimestamp(time_left + current_time).strftime("%m/%d/%Y %H:%M:%S")

    def run(self):
        """
        Method that kicks off the downloading
        """
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
        LOGGER.info("Created Thread pool")

        while True:
            # If queue is empty break out of loop
            if self.queue.empty():
                self.queue.join()
                break

        end_time = time.perf_counter()
        LOGGER.info("Downloaded {file_count} in {time} secs".
                    format(file_count=self.file_count, time=(end_time - start_time)))


def main(args):
    """
    Initializes an instance of the downloader
    :param args: Config Path
    """
    download = InternetArchiveDownloader(args[1])
    download.run()


if __name__ == "__main__":
    main(sys.argv)
