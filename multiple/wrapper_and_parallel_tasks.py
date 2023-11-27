import luigi

class DownloadSalesData(luigi.Task):
    def output(self):
        return luigi.LocalTarget('all_sales.csv')

    def run(self):
        with self.output().open('w') as f:
            print('France,May,100', file=f)
            print('Germany,May,120', file=f)
            print('France,June,150', file=f)
            print('Germany,June,180', file=f)


class GetFranceSales(luigi.Task):
    def requires(self):
        return DownloadSalesData()

    def output(self):
        return luigi.LocalTarget('france_sales.csv')

    def run(self):
        with self.output().open('w') as out:
            with self.input().open() as f:
                for line in f:
                    if line.startswith('France'):
                        out.write(line)


class SummarizeFranceSales(luigi.Task):
    def requires(self):
        return GetFranceSales()

    def output(self):
        return luigi.LocalTarget('summary_france_sales.csv')

    def run(self):
        total = 0
        with self.input().open() as f:
            for line in f:
                aa = line.split(',')
                total += float(aa[2])

        with self.output().open('w') as out:
            out.write(str(total))


class GetGermanySales(luigi.Task):
    def requires(self):
        return DownloadSalesData()

    def output(self):
        return luigi.LocalTarget('germany_sales.csv')

    def run(self):
        with self.output().open('w') as out:
            with self.input().open() as f:
                for line in f:
                    if line.startswith('Germany'):
                        out.write(line)


class SummarizeGermanySales(luigi.Task):
    def requires(self):
        return GetGermanySales()

    def output(self):
        return luigi.LocalTarget('summary_germany_sales.csv')

    def run(self):
        total = 0
        with self.input().open() as f:
            for line in f:
                aa = line.split(',')
                total += float(aa[2])

        with self.output().open('w') as out:
            out.write(str(total))


class Final(luigi.WrapperTask):
    def requires(self):
        return [SummarizeFranceSales(),
                SummarizeGermanySales()]


if __name__ == '__main__':
    luigi.run(['Final'])