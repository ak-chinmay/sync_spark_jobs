from src import arg_parser
from src.jobs.event import Event
from src.jobs.game import Game

job_mapping = {"EVENT": ["get_players_with_most_events", "get_pivoted_events_type_stats_by_month",
                         "get_pivoted_events_type_stats_by_year"],
               "GAME": ["get_region_with_most_games", "get_region_with_most_games_in_dates", "get_stats_for_each_region"]}


def validate_job(scope: str, job: str):
    if scope not in job_mapping:
        raise ValueError(f"Scope not present, please enter from: {job_mapping.keys()}")
    if job not in job_mapping[scope]:
        raise ValueError("Job not valid, please enter valid job")


def initialize_scope_object(scope, input_files):
    switch = {
        "EVENT": Event(input_files),
        "GAME": Game(input_files)
    }
    if input_files is None or len(input_files) == 0:
        raise ValueError("No Input files provided")
    return switch.get(scope)


if __name__ == '__main__':
    args = arg_parser.get_args()
    print(args)
    scope = str(args.scope).upper()
    list_jobs = args.list_jobs
    if scope is not None and scope in job_mapping and list_jobs:
        print(f"Scope: {scope} Jobs: {job_mapping[scope]}")
    job = args.job
    if job is not None:
        validate_job(scope, job)
    input_files = args.inputfiles
    scope_obj = initialize_scope_object(scope, input_files)

    arguments = args.arguments
    export_path = args.exportpath
    arg_dict = {'export_path': export_path, 'arguments': arguments}
    method = getattr(scope_obj, job)
    method(arg_dict)


