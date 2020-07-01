# Covid Registry

---

Covid registry allows the extraction of tables of registered deaths caused by *COVID19* and other, possibly related causes in Brazil. The relevant data is provided by the [civil registry](https://transparencia.registrocivil.org.br/inicio).

## Usage

Either query based on a given date, state, city, gender and place of death and get the result as a Pandas DataFrame:

```python
from covid_registry import CovidRegistry
from datetime import date

async with CovidRegistry() as reg:
    d = date(2020, 5, 6)
    state = 'RJ'
    city = 'Rio de Janeiro'
    gender = 'M'
    place_of_death = 'HOSPITAL'
    
    df = await reg.query(
        date=d,
        state=state,
        city=city,
        gender=gender,
        place_of_death=place_of_death,
        include_cardiac=True
    )
```

or extract whole ranges of data directly to a file:

```python
from covid_registry import CovidRegistry
import pandas as pd

name = 'sao_paulo_sao_paulo.csv'
pkl = 'remaining_tasks.pkl'

async with CovidRegistry() as reg:
    dates = pd.date_range(start='20200101', end='20200629')
    await reg.dump(dates, states=['SP'], cities=['São Paulo'], include_cardiac=True, file=name, pkl=pkl)
```

Check the `examples` folder for more details.