import json
from pathlib import Path

def load():
    return json.loads((Path(__file__).with_name("context.json")).read_text())

def run():
    from hangar_ingest.runs.database import Database
    from hangar_ingest.api import Application
    data = load()
    setup = json.loads((Path(__file__).parents[1] / 'fixtures/demo.json').read_text())['setup']
    with Database(':memory:') as db:
        app = Application(db)
        for request in setup:
            app.call(request['operation'], request['parameters'])
        text = ''.join(json.dumps(r) + '\n' for r in data['records'])
        app.call('batch.stage', {'batch_id': 'monitoring', 'source': 'feed1', 'content': text})
        app.call('batch.drain', {'batch_id': 'monitoring'})
        n = db.scalar('SELECT COUNT(*) FROM observations')
        assert n == len(data['records'])
        return {'ok': app.call('inspect.check')['ok'], 'records': n}


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, sort_keys=True))
