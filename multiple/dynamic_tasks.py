import os
import luigi

INPUT_FOLDER = 'input'
OUTPUT_FOLDER = 'output'


class DownloadFile(luigi.Task):
    file_name = luigi.Parameter()

    def output(self):
        path = os.path.join(OUTPUT_FOLDER, self.file_name)
        return luigi.LocalTarget(path)

    def run(self):
        input_path = os.path.join(INPUT_FOLDER, self.file_name)
        with open(input_path) as f:
            with self.output().open('w') as out:
                for line in f:
                    if ',' in line:
                        out.write(line)


class DownloadSalesData(luigi.Task):
    def output(self):
        return luigi.LocalTarget('all_sales.csv')

    def run(self):
        processed_files = []
        for file in os.listdir(INPUT_FOLDER):
            print("Filename: ", {file})
            target = yield DownloadFile(file)
            processed_files.append(target)

        with self.output().open('w') as out:
            for file in processed_files:
                with file.open() as f:
                    for line in f:
                        out.write(line)


if __name__ == '__main__':
    luigi.run(['DownloadSalesData'])
