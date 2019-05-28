from functions import SessionParameters, set_logging, RunParameters

from pull_data import run_transactions


if __name__ == "__main__":

    run_params = RunParameters()

    start = set_logging(run_params)  # maakt een logbestand aan en bepaald starttijd

    login = SessionParameters(user='Python',
                              pw=r'U3RhZmZpbmcyMDE5IQ==\n',
                              organisation='Associates')

    jaren = ['2018']

    for jaar in jaren:

        data = run_transactions(run_params,
                                login = login,
                                start = start,
                                jaar = jaar,
                                upload = True)
