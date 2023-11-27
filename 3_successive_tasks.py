#unique name
#Atomic: Basic unit of work
#Indepodent 

import luigi

class ProcessOrders(luigi.Task):
    def output(self):
        return luigi.LocalTarget('order.csv')
    
    def run(self):
        with self.output().open('w') as f:
            print('May,100', file=f)
            print('May,180', file=f)
            print('Jun,200', file=f)
            print('Jun,150', file=f)



class GenerateReport(luigi.Task):
    def requires(self):
        return ProcessOrders()
        
    def output(self):
        return luigi.LocalTarget('report.csv')
    
    def run(self):
        report={}
        for line in self.input().open():
            month,amount = line.split(',')
            if month in report:
                report[month] += float(amount)
            else:
                report[month] = float(amount)
        
        with self.output().open('w') as out:
            for month in report:
                print(month + ',' + str(report[month]), file=out)


class SummarizeReport(luigi.Task):
    def requires(self):
        return GenerateReport()
    
    def output(self):
        return luigi.LocalTarget('summary.txt')
    
    def run(self):
        total = 0.0
        for line in self.input().open():
            month, amount = line.split(',')
            total += float(amount)

            with self.output().open('w') as f:
                f.write(str(total))

if __name__ == '__main__':
     luigi.run(['SummarizeReport', '--local-scheduler'])        # specify what task to start and where 
                                                                # --local-scheduler, means that is does not connect to the service and run here
                                                                # possible to run 

        

