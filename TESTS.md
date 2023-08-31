
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


Current issues:
- Django generates queries with parameter markers for literals, eg. "SELECT ?
  FROM SYSIBM.SYSDUMMY1", ["foo"] This isn't supported by Db2, but can be when
  inside a CAST, eg. "SELECT CAST(? AS VARCHAR(10)) FROM SYSIBM.SYSDUMMY1",
  ["foo"]

  Need to figure out how to deal with this as by the time it gets to
  cursor.execute() we don't know that the parameter marke is in the query list.

- Timestamp handling needs to be gone through with a fine-toothed comb to see
  how to handle this correctly

- Various tests fail due to SQL0910 errors: "Object &1 in &2 type *&3 has a
  pending change." Because test cases run in a transaction,
  eg. backends.tests.SequenceResetTest.test_generic_relation creates a Post
  record, then tries to reset the sequence, then create another record. But
  because the Post table has pending changes, you can't reset the sequence
  number while that's going on.

- sql_flush needs a big rethink. The current code creates a stored procedure
  which uses LUW specific syntax to disable the foreign key constraints (ALTER
  TABLE ... ALTER FOREIGN KEY ... NOT ENFORCED), then delete the rows, then
  re-enable the foreign key constraints using the stored procedure, then drop
  the procedure. Other databases seem to have similar features or the ability
  to disable foreign key constraints for a transaction (MySQL), but I don't see
  any way to do this in Db2 for i.

  The only way I can think we can do it is if we drop the foreign keys, then
  delete the rows, then add the foreign keys back. This of course requires
  recording all the details about how they were defined before dropping them
  so we can recreate them exactly, ugh.

- django.db.models.expressions.Exists expression can't really be implemented on
  Db2 for i. EXISTS is only supported in a WHERE clause, so we'd need a scalar
  function be implemented eg. SYSTOOLS.EXISTS(). Once that exists, we can patch
  in custom behavior by adding an "as_db2" method to the class.
