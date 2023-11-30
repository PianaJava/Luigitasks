import os

import luigi
from luigi import Task, Parameter, LocalTarget, IntParameter, DictParameter

OUTPUT_FOLDER = 'output'


class DownloadFile(Task):
    input_folder = Parameter()
    file_name = Parameter()
    index = IntParameter()

    def output(self):
        path = os.path.join(OUTPUT_FOLDER,
                            str(self.index),
                            self.file_name)
        return LocalTarget(path)

    def run(self):
        from time import sleep
        sleep(5)
        input_path = os.path.join(self.input_folder,
                                  self.file_name)
        with open(input_path) as f:
            with self.output().open('w') as out:
                for line in f:
                    if ',' in line:
                        out.write(line)


class DownloadSalesData(Task):
    params = DictParameter()

    def output(self):
        return LocalTarget(self.params['output'])

    def run(self):
        processed_files = []
        counter = 1
        input_files = sorted(os.listdir(self.params['input']))
        tasks = []
        for file in input_files:
            tasks.append(DownloadFile(self.params['input'],
                                        file, counter))
            self.set_progress_percentage(100 * counter / len(input_files))
            counter += 1

        processed_files = yield tasks

        with self.output().open('w') as out:
            for file in processed_files:
                with file.open() as f:
                    for line in f:
                        out.write(line)


if __name__ == '__main__':
    luigi.run(['DownloadSalesData'])
