from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

class SalesAnalysis(MRJob):

    def configure_args(self):
        super(SalesAnalysis, self).configure_args()
        self.add_passthru_arg('--question', default='1', help='Choose the question (1, 2, 3, or 4)')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_question1, reducer=self.reducer_question1),
            MRStep(mapper=self.mapper_question2, reducer=self.reducer_question2),
            MRStep(mapper=self.mapper_question3, reducer=self.reducer_question3),
            MRStep(mapper=self.mapper_question4, reducer=self.reducer_question4)
        ]

    def parse_datetime(self, datetime_str):
        try:
            dt = datetime.strptime(datetime_str, '%m-%d-%y %H:%M:%S')
            year = dt.year
            return year
        except ValueError:
            # Handle invalid datetime strings here if needed
            return None

    def mapper_question1(self, _, line):
        fields = line.strip().split(',')
        _, _, country, item_type, _, _, order_date, _, _, _, unit_price, _, _, _, _ = fields
        year = self.parse_datetime(order_date)
        if year is not None:
            yield (year, country, item_type), (float(unit_price), 1)

    def reducer_question1(self, key, values):
        total_price = 0
        total_count = 0
        for price, count in values:
            total_price += price
            total_count += count
        avg_price = total_price / total_count
        yield key, avg_price

    def mapper_question2(self, _, line):
        fields = line.strip().split(',')
        _, _, country, item_type, _, _, order_date, _, _, units_sold, _, _, _, _, _ = fields
        year = self.parse_datetime(order_date)
        if year is not None:
            yield (year, country, item_type), int(units_sold)

    def reducer_question2(self, key, values):
        total_units_sold = sum(values)
        yield key, total_units_sold

    def mapper_question3(self, _, line):
        fields = line.strip().split(',')
        _, _, country, item_type, _, _, order_date, _, _, units_sold, _, _, _, _, _ = fields
        year = self.parse_datetime(order_date)
        if year is not None:
            yield (year, country, item_type), int(units_sold)

    def reducer_question3(self, key, values):
        max_units_sold = max(values)
        min_units_sold = min(values)
        yield key, (max_units_sold, min_units_sold)
    
    def mapper_question4(self, _, line):
        fields = line.strip().split(',')
        _, _, _, _, _, _, order_date, order_id, _, _, unit_price, _, _, _, _ = fields
        year = self.parse_datetime(order_date)
        if year is not None:
            total_profit = float(unit_price)
            yield (year, order_id), total_profit

    def reducer_question4(self, key, values):
        top_10 = sorted(values, reverse=True)[:10]
        yield key[0], (key[1], top_10)

if __name__ == '__main__':
    SalesAnalysis.run()