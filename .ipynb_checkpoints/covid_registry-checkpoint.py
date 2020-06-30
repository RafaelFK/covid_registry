import aiohttp
import asyncio
import time
import pickle
import pandas as pd
import numpy as np
from yarl import URL
from datetime import date, timedelta
from pathlib import Path
from tqdm import tqdm
import traceback


class CovidRegistry:
    def __init__(self):
        self.base_url = 'https://transparencia.registrocivil.org.br'
        self.landing_url = 'https://transparencia.registrocivil.org.br/especial-covid'
        self.api_url = 'https://transparencia.registrocivil.org.br/api/covid-covid-registral'
        self.base_headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:76.0) Gecko/20100101 Firefox/76.0'
        }
        self.session = None
        self.cities = None
        self.min_date = date(year=2020, month=1, day=1)
        self.max_date = date.today()
        self.places_of_death = pd.Series(
            data=['HOSPITAL', 'DOMICILIO', 'VIA_PUBLICA', 'AMBULANCIA', 'OUTROS'])
        self.genders = pd.Series(data=['M', 'F'])
        self.columns = ['Date', 'State', 'City', 'Age',
                        'Gender', 'Place of Death', 'Cause', '#']

    async def connect(self):
        if self.session is not None:
            await self.close()

        self.session = aiohttp.ClientSession(headers=self.base_headers)

        # Get session and XSRF token
        # TODO: check rensponse status
        await self.session.get(self.landing_url)

        if self.cities is None:
            await self.get_cities()

    async def get_cities(self, force_update=True):
        if self.session is None:
            raise Exception('A connection must be established first!')

        url = 'https://transparencia.registrocivil.org.br/api/cities'

        if force_update or self.cities is None:
            self.cities = pd.DataFrame.from_dict(
                (await (await self._get(url)).json())['cities']
            )

        return self.cities

    async def query(self, date=date.today(), state='RJ', city='Rio de Janeiro', gender='M', place_of_death='HOSPITAL'):
        if self.session is None:
            raise Exception('A connection must be established first!')

        if not (self.min_date <= date <= self.max_date):
            raise Exception('Date should be in the valid range!')

        state, city, city_id = await self._state_and_city_keys(state, city)

        gender = self._gender_key(gender)
        chart = 'chart2' if gender == 'M' else 'chart3'

        place_of_death = self._place_of_death_key(place_of_death)

        # Make request
        params = {
            'start_date': f'{date}',
            'end_date': f'{date}',
            'state': f'{state}',
            'city_id': f'{city_id}',
            'chart': f'{chart}',
            'gender': f'{gender}',
            'places[]': f'{place_of_death}'
        }

        j = (await (await self._get(self.api_url, params)).json())['chart']
        return self._json_to_dataframe(
            json=j,
            d=date,
            state=state,
            city=city,
            gender=gender,
            place_of_death=place_of_death
        )

    # TODO: Should write csv and pickle before first chunk. Otherwise, I wont be able to resume a dump that was canceled before that
    async def dump(self, timerange, states=None, cities=None, places_of_death=None, checkpoint_once_in=200, file='results.csv', pkl='remaining_tasks.pkl', print_trace=False):

        # Create the list of tasks to be executed
        coroutines_args = []
        for d in timerange.to_pydatetime():
            d = d.date()
            state_range = self.cities['uf'].unique(
            ) if states is None else states
            for state in state_range:
                cities_range = self.cities[self.cities['uf'] ==
                                           state]['name'] if cities is None else cities
                for city in cities_range:
                    for gender in self.genders:
                        places_range = self.places_of_death if places_of_death is None else places_of_death
                        for place in places_range:
                            coroutines_args.append({
                                'date': d,
                                'state': state,
                                'city': city,
                                'gender': gender,
                                'place_of_death': place
                            })

        # Create initial table and remaining tasks pickle
        pd.DataFrame(columns=self.columns).to_csv(file, index=False)
        with open(pkl, 'wb') as o:
            pickle.dump(coroutines_args, o)

        # Execute tasks
        await self._dump_tasks(file, pkl, coroutines_args, checkpoint_once_in, print_trace=print_trace)

    async def resume_dump(self, data_up_until_now, remaining_tasks_pickle, checkpoint_once_in=200, print_trace=False):
        # Deserialize pickle
        with open(remaining_tasks_pickle, 'rb') as file:
            coroutines_args = pickle.load(file)

        await self._dump_tasks(data_up_until_now, remaining_tasks_pickle, coroutines_args, checkpoint_once_in, print_trace=print_trace)

    async def _dump_tasks(self, datafile, pkl, coroutines_args, checkpoint_once_in=200, print_trace=False):
        failures = []
        epoch = time.time()
        df = pd.DataFrame(columns=self.columns)

        while len(coroutines_args) > 0:
            print(
                f'This operation will take {len(coroutines_args)} requests. Checkpoints will be created once every {checkpoint_once_in} requests.')

            for idx, co_chunk in enumerate(chunks(coroutines_args, of_size=checkpoint_once_in)):
                # Open connection for chunk. This is an attempt to deal with 'server disconnected' issues
                await self.connect()

                results = await asyncio.gather(
                    *map(lambda args: self.query(**args), co_chunk),
                    return_exceptions=True
                )

                new_failures = [t for t in filter(
                    lambda t: type(t[1]) is not pd.DataFrame,
                    zip(co_chunk, results)
                )]

                failures.extend(new_failures)

                successes = filter(lambda r: type(r) is pd.DataFrame, results)

                print(f'#{idx+1:04d} -- {idx*checkpoint_once_in + len(co_chunk)}/{len(coroutines_args)} requests -- {len(new_failures)} failures -- Elapsed: {timedelta(seconds=time.time()-epoch)}')

                if len(new_failures) > 0:
                    unique_errors, indices = np.unique(
                        list(map(lambda t: str(type(t[1])), new_failures)), return_index=True)
                    print('Errors found in chunk:')
                    print(
                        f'{unique_errors}')

                    if print_trace:
                        for idx in indices:
                            exc = new_failures[idx][1]
                            traceback.print_exception(
                                type(exc), exc, exc.__traceback__)

                point = df.append(list(successes), ignore_index=True)

                # Saving current results
                point.to_csv(datafile, mode='a', header=False, index=False)

                # Saving remaining tasks
                remaining_tasks = []
                # Tasks that weren't reached yet
                remaining_tasks.extend(
                    coroutines_args[(idx*checkpoint_once_in + len(co_chunk)):])
                # Tasks that failed
                remaining_tasks.extend([c for c, _ in failures])

                if len(remaining_tasks) > 0:
                    with open(pkl, 'wb') as o:
                        pickle.dump(remaining_tasks, o)
                else:
                    f = Path(pkl)
                    if f.is_file():
                        f.unlink()

                # Close connection for chunk
                await self.close()

            coroutines_args = []
            if len(failures) > 0:
                print(f'{len(failures)} failures occured. Retrying...')
                coroutines_args = [c for c, _ in failures]
                failures = []

    def _json_to_dataframe(self, json, d, state, city, gender, place_of_death):
        df = pd.DataFrame(columns=self.columns)

        if type(json) is list and not json:
            return df

        for age in json.keys():
            for year in json[age].keys():
                if year == '2019':
                    if d.month == 2 and d.day == 29:
                        # the site seems to believe this date exists
                        continue
                    dd = date(year=2019, month=d.month, day=d.day)
                else:
                    dd = d

                for cause in json[age][year].keys():
                    df = df.append({
                        'Date': dd,
                        'State': state,
                        'City': city,
                        'Age': age,
                        'Gender': gender,
                        'Place of Death': place_of_death,
                        'Cause': cause,
                        '#': json[age][year][cause]
                    }, ignore_index=True)

        return df

    # Internal get. Appends XSRF token

    async def _get(self, url, params=None):
        if self.session is None:
            raise Exception('A connection must be established first!')
        res = await self.session.get(
            url,
            params=params,
            headers={
                'X-XSRF-TOKEN': self.session.cookie_jar.filter_cookies(self.base_url)['XSRF-TOKEN'].value
            }
        )

        res.raise_for_status()
        return res

    async def _state_and_city_keys(self, state, city):
        '''Validate and get the values in the format expected by the API'''
        if self.cities is None:
            await self.get_cities()

        res = self.cities[
            self.cities['uf'].str.contains(state, case=False) &
            self.cities['name'].str.contains(city, case=False)]

        if res.empty:
            raise Exception(
                f'<State-city> combination <{state}-{city}> not found!')

        return res['uf'].values[0], res['name'].values[0], res['id'].values[0]

    def _gender_key(self, gender):
        '''Validate and get the value in the format expected by the API'''
        pattern = self.genders.str.contains(gender, case=False)
        res = self.genders[pattern]

        if res.empty:
            raise Exception(f'{gender} is not a valid gender!')

        return res.values[0]

    def _place_of_death_key(self, place_of_death):
        '''Validate and get the value in the format expected by the API'''
        pattern = self.places_of_death.str.contains(place_of_death, case=False)
        res = self.places_of_death[pattern]

        if res.empty:
            raise Exception(f'{place_of_death} is not a valid place of death!')

        return res.values[0]

    async def close(self):
        if self.session is not None:
            await self.session.close()

    async def __aenter__(self):
        # Stablish session. Get token
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        # # Close session. Needs to be async?
        await self.close()


class RequestFailedError(Exception):
    def __init__(self, msg, request_info):
        self.msg = msg
        self.request_info = request_info

    def __str__(self):
        return f'{self.msg}'


def chunks(arr, of_size=1):
    if len(arr) <= of_size:
        yield arr
    else:
        n = len(arr)//of_size

        for chunk in np.array_split(arr[:of_size*n], n):
            yield chunk

        yield arr[of_size*n:]
