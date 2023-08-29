
git clone https://github.com/django/django.git
cd django
git worktree add ../django-2.2 stable/2.2.x
git worktree add ../django-3.2 stable/3.2.x
...


cd ../django-2.2
python3 -m venv .venv
. .venv/bin/activate

python -m pip install -r tests/requirements/py3.txt

# On macOS ARM64
# https://stackoverflow.com/a/75190318
pip install --no-binary :all: pyodbc


python -m pip install ~/projects/django-ibmi

cd tests
python -m pip install -e ..

# Base test (sqlite)
./runtests.py

# Test IBM i
./runtests.py --settings test_ibmi


Current issues is need to implement create_test_db / destroy_test_db.
This requires mapping "database" concept in Django to something we can create,
with closest equivalent being a schema.
