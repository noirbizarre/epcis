#!/usr/bin/env python
import csv
import itertools
import json

from dataclasses import dataclass
from datetime import date

from tinydb import TinyDB, Query
from tinydb.storages import MemoryStorage

FIRST_YEAR = 1999
LAST_YEAR = 2019
OUTPUT = 'historique-epcis.json'

DOT_MOD = 10
IDX_MOD = 10 * DOT_MOD
MISMATCH_ERROR = 'Number of members mismatch for "{0}", expected {1}, found {2}'
SUMMARY = '''Processed {total} EPCIs on {years} years.
- {insertions} insertions
- {updates} updates
- {ended} ended
'''

INSERTED, UPDATED = range(2)

EPCI = Query()


def color(code):
    '''A simple ANSI color wrapper factory'''
    return lambda t: '\033[{0}{1}\033[0;m'.format(code, t)


green = color('1;32m')
red = color('1;31m')
purple = color('1;35m')
white = color('1;39m')


def info(text, *args, **kwargs):
    '''Display informations'''
    text = text.format(*args, **kwargs)
    print(' '.join((purple('➤'), white(text))))


def success(text, *args, **kwargs):
    '''Display a success message'''
    text = text.format(*args, **kwargs)
    print(' '.join((green('✓'), white(text))))


def error(text, *args, **kwargs):
    '''Display an error message'''
    text = text.format(*args, **kwargs)
    print(red('✘ {0}'.format(text)))


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        elif isinstance(obj, date):
            return obj.isoformat()
        return super().default(obj)


@dataclass
class LoadResult:
    total: int = 0
    new: int = 0
    updated: int = 0
    ended: int = 0


class Store:
    def __init__(self):
        self.db = TinyDB(storage=MemoryStorage)

    def load_year(self, year, filename):
        sirens = set()
        result = LoadResult()
        with open(filename) as infile:
            reader = csv.DictReader(infile, delimiter=';', quotechar='"')
            groups = itertools.groupby(reader, key=lambda r: r['siren'])
            for idx, (siren, rows) in enumerate(groups, 1):
                if idx % DOT_MOD == 0:
                    print(white(idx) if idx % IDX_MOD == 0 else '.', end='', flush=True)
                op = self.upsert(year, siren, list(rows))
                if op == INSERTED:
                    result.new += 1
                elif op == UPDATED:
                    result.updated +=1
                result.total += 1
                sirens.add(siren)
        print('')
        # cleanup missing SIRENs
        ids = self.db.update(
            {'dateFin': date(year - 1, 12, 31)},
            ~(EPCI.siren.one_of(sirens) | EPCI.dateFin.exists())
        )
        result.ended = len(ids)
        return result

    def upsert(self, year, siren, rows):
        current = self.db.get((EPCI.siren == siren) & (~(EPCI.dateFin.exists())))
        epci = self.extract_epci(year, rows)
        if current is None:
            self.db.insert(epci)
            return INSERTED
        if bool(current['membres'] ^ epci['membres']):
            epci['predecesseurs'] = [current['id']]
            self.db.insert(epci)
            self.db.update({
                'successeurs': [epci['id']],
                'dateFin': date(year - 1, 12, 31),
                'raisonFin': 'Changement de membres',
            }, doc_ids=[current.doc_id])
            return UPDATED

    def extract_epci(self, year, rows):
        row = rows[0]
        epci = {
            'id': '{siren}@{year}-01-01'.format(year=year, **row),
            'dateDebut': date(year, 1, 1),
            'siren': row['siren'],
            'nom': row['nom'],
            'nature': row['nature'],
            'fiscalite': row['fiscalite'],
            'population': row['ptot'],
            'membres': {r['insee'].zfill(5) for r in rows},
        }
        if int(row['nb_com']) != len(epci['membres']):
            error(MISMATCH_ERROR, epci['siren'], row['nb_com'], len(epci['membres']))
        return epci

    def dump_to(self, filename):
        with open(filename, 'w') as out:
            json.dump(self.db.all(), out, cls=JSONEncoder)


def build_history(output):
    '''Build the history JSON file'''
    store = Store()
    results = []

    for year in range(FIRST_YEAR, LAST_YEAR + 1):
        filename = '{}.csv'.format(year)
        info('Processing {}', filename)
        result = store.load_year(year, filename)
        success('Processed {r.total} EPCIs: added {r.new}, updated {r.updated}, ended {r.ended}', r=result)
        results.append(result)

    info('Writing history file')
    store.dump_to(output)
    success('History writed to {}', output)

    # Print summary
    print('=' * 80)
    success(SUMMARY,
            total=sum(r.total for r in results),
            insertions=sum(r.new for r in results),
            updates=sum(r.updated for r in results),
            ended=sum(r.ended for r in results),
            years=len(results)
            )


if __name__ == '__main__':
    build_history(OUTPUT)
