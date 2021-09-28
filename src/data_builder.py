import json
import os
import sys
from datetime import datetime

from faker import Faker
import collections


class DataBuilder:
    database = []
    # filename = '1M'
    # length = 1000000
    fake = Faker()
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    def fake_player_generator(self, length):
        for x in range(length):
            yield collections.OrderedDict([
                ('id', self.fake.random_int(min=1, max=1000000)),
                ('last_name', self.fake.last_name()),
                ('first_name', self.fake.first_name()),
                ('address', self.fake.address()),
                ('email', self.fake.email()),
                ('phone', self.fake.phone_number()),
                ('description', self.fake.text())
            ])
    def fake_game_generator(self, length):
        game_types = ['POKER', 'BLACKJACK', 'BACCARAT', 'OMAHA']
        for x in range(length):
            yield collections.OrderedDict([
                ('game_id', self.fake.random_int(min=1, max=100000)),
                ('location', self.fake.city()),
                ('type', game_types[self.fake.random_int(min=0, max=3)]),
                ('started_at', self.fake.date_time().strftime(self.DATE_FORMAT)),
                ('tournament_id', self.fake.random_int(min=1, max=1000))
            ])

    def fake_event_generator(self, length):
        event_types = ['CALL', 'FOLD', 'RAISE']
        for x in range(length):
            yield collections.OrderedDict([
                ('event_id', self.fake.random_int(min=1, max=1000000)),
                ('game_id', self.fake.random_int(min=1, max=100000)),
                ('player_id', self.fake.random_int(min=1, max=1000000)),
                ('event_type', event_types[self.fake.random_int(min=0, max=2)]),
                ('event_time', self.fake.date_time().strftime(self.DATE_FORMAT))
            ])


    def generate(self, length, generator_func):
        return generator_func(length)

    def export(self, filename, length, generator_func):
        fake_objects = self.generate(length, generator_func)
        counter = 1
        with open(filename, 'w') as output:
            for obj in fake_objects:
                # if counter == 1:
                #     output.write("[")
                json.dump(obj, output)
                output.write("\n")
                if counter % 50000 == 0:
                    print(counter)
                counter = counter + 1
        print("Done.")

    # def run_parallel(self, filepath, length, threads):
    #     for x in range(threads):
    #         filepath = f"{self.fake.random_int(min=1, max=threads)}_{datetime.utcnow().strftime('%d%m%y_%H%M')}_{filepath}"
    #         data_builder.export(filepath, length)

    def get_size_of_file(self, filepath: str) -> int:
        return os.path.getsize(filepath)


if __name__ == '__main__':
    # filepath = "../export/people_aug_2021.json"
    # length = 10000000
    # filepath = sys.argv[0]
    # length = sys.argv[1]
    # threads = sys.argv[2]
    data_builder = DataBuilder()
    data_builder.export("../export/event_sept_2021.json", 1000, data_builder.fake_event_generator)
    # size = data_builder.get_size_of_file(filepath)
    # print(f"File with size: {size / 1000} KB exported")
