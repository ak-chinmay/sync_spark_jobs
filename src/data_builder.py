import json
import os
import sys
from datetime import datetime

from faker import Faker
import collections


class DataBuilder:
    database = []
    fake = Faker()
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    def fake_player_generator(self, num_records: int):
        """
        This method generates the fake player records
        :param num_records: Number of records to be exported
        :return: Generator object
        """
        for x in range(num_records):
            yield collections.OrderedDict([
                ('id', self.fake.random_int(min=1, max=1000000)),
                ('last_name', self.fake.last_name()),
                ('first_name', self.fake.first_name()),
                ('address', self.fake.address()),
                ('email', self.fake.email()),
                ('phone', self.fake.phone_number()),
                ('description', self.fake.text())
            ])

    def fake_game_generator(self, num_records):
        """
        This method generates the fake game records
        :param num_records: Number of records to be exported
        :return: Generator object
        """
        game_types = ['POKER', 'BLACKJACK', 'BACCARAT', 'OMAHA']
        for x in range(num_records):
            yield collections.OrderedDict([
                ('game_id', self.fake.random_int(min=1, max=100000)),
                ('location', self.fake.city()),
                ('type', game_types[self.fake.random_int(min=0, max=3)]),
                ('started_at', self.fake.date_time().strftime(self.DATE_FORMAT)),
                ('tournament_id', self.fake.random_int(min=1, max=1000))
            ])

    def fake_event_generator(self, num_records: int):
        """
        This method generates the fake event records
        :param num_records: Number of records to be exported
        :return: Generator object
        """
        event_types = ['CALL', 'FOLD', 'RAISE']
        for x in range(num_records):
            yield collections.OrderedDict([
                ('event_id', self.fake.random_int(min=1, max=1000000)),
                ('game_id', self.fake.random_int(min=1, max=100000)),
                ('player_id', self.fake.random_int(min=1, max=1000000)),
                ('event_type', event_types[self.fake.random_int(min=0, max=2)]),
                ('event_time', self.fake.date_time().strftime(self.DATE_FORMAT))
            ])

    def generate(self, num_records: int, generator_func):
        """
        This method is decorator based generic method which executes appropriate method.
        :param num_records: Number of records to be exported
        :param generator_func: Generator function
        :return: Return value of function i.e. Generator object
        """
        return generator_func(num_records)

    def export(self, filename: str, num_records: int, generator_func):
        """
        This method is a wrapper for the complete business logic
        :param filename:
        :param num_records:
        :param generator_func:
        :return:
        """
        fake_objects = self.generate(num_records, generator_func)
        counter = 1
        with open(filename, 'w') as output:
            for obj in fake_objects:
                json.dump(obj, output)
                output.write("\n")
                if counter % 50000 == 0:
                    print(counter)
                counter = counter + 1
        print("Done.")

    def get_size_of_file(self, filepath: str) -> int:
        """
        This method returns the size of the filepath
        :param filepath: Input filepath
        :return: Size
        """
        return os.path.getsize(filepath)


if __name__ == '__main__':
    try:
        data_builder = DataBuilder()
        entity_mapping = {"PLAYER": data_builder.fake_player_generator,
                          "EVENT": data_builder.fake_event_generator,
                          "GAME": data_builder.fake_game_generator}
        if len(sys.argv) < 4:
            raise ValueError("ERROR: Please enter all the arguments, 1: Filepath, 2. Number of records, 3.Entity \n"
                             "e.g. python src/data_builder.py ./export/dummy_player.json 1000 PLAYER")
        print(sys.argv)
        filepath = sys.argv[1]
        num_records = 1000
        try:
            num_records = int(sys.argv[2])
        except Exception as ex:
            print(f"WARNING: Invalid number of records `{sys.argv[2]}`, setting it to default: 1000")
            num_records = 1000
        entity = sys.argv[3].upper()
        if entity not in entity_mapping:
            raise ValueError(f"ERROR: Please enter valid entity from {entity_mapping.keys()}")
        entity_gen_func = entity_mapping[entity]
        data_builder.export(filepath, num_records, entity_gen_func)
        # data_builder.export("../export/event_sept_2021.json", 1000, data_builder.fake_event_generator)
    except Exception as ex:
        print(ex)
