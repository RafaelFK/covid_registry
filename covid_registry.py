import aiohttp
import pandas as pd
from yarl import URL
from datetime import date

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
        self.places_of_death = pd.Series(data=['HOSPITAL', 'DOMICILIO', 'VIA_PUBLICA', 'AMBULANCIA', 'OUTROS'])
        self.genders = pd.Series(data=['M', 'F'])

    async def connect(self):
        self.session = aiohttp.ClientSession(headers=self.base_headers)

        # Get session and XSRF token
        # TODO: check rensponse status
        await self.session.get(self.landing_url)

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

        state, city_id = await self._state_and_city_keys(state, city)

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

        return (await (await self._get(self.api_url, params)).json())['chart']
        

    # Internal get. Appends XSRF token
    async def _get(self, url, params=None):
        if self.session is None:
            raise Exception('A connection must be established first!')

        return await self.session.get(
            url,
            params=params,
            headers={
                'X-XSRF-TOKEN': self.session.cookie_jar.filter_cookies(self.base_url)['XSRF-TOKEN'].value
            }
        )


    async def _state_and_city_keys(self, state, city):
        '''Validate and get the values in the format expected by the API'''
        if self.cities is None:
            await self.get_cities()

        res = self.cities[
            self.cities['uf'].str.contains(state, case=False) & 
            self.cities['name'].str.contains(city, case=False)]

        if res.empty:
            raise Exception(f'<State-city> combination <{state}-{city}> not found!')

        return res['uf'].values[0], res['id'].values[0]


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
        pass

    async def __aexit__(self, exc_type, exc, tb):
        # # Close session. Needs to be async?
        pass
    

