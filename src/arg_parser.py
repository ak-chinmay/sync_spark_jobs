import argparse


def get_args():
    """
    This method holds the business logic of parsing the argument
    :return: args the object holding the user arguments
    """
    parser = argparse.ArgumentParser(description='Sync computing | Spark Jobs')
    parser.add_argument('-s', '--scope', help='Scope of job', required=True)
    parser.add_argument('-j', '--job', help='Name of job', required=False)
    parser.add_argument('-a', '--arguments', nargs='+',
                        help='This parameter is to provide data source specific dynamic arguments')
    parser.add_argument('-files', '--inputfiles', nargs='+',
                        help='This parameter is to provide data source specific dynamic arguments')
    parser.add_argument('-ep', '--exportpath',
                        help="""This export path 
                            i.e. <export_path>
                            e.g.  /tmp/games/ """)
    parser.add_argument('-l', '--listjobs',
                        help='This flag shows the CLI Arguments info for a data source e.g. `-s GAME -i`',
                        dest="list_jobs", action="store_true", default=False)
    return parser.parse_args()
