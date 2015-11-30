"""
Peewee integration with APSW, "another python sqlite wrapper".

Project page: https://code.google.com/p/apsw/

APSW is a really neat library that provides a thin wrapper on top of SQLite's
C interface.

Here are just a few reasons to use APSW, taken from the documentation:

* APSW gives all functionality of SQLite, including virtual tables, virtual
  file system, blob i/o, backups and file control.
* Connections can be shared across threads without any additional locking.
* Transactions are managed explicitly by your code.
* APSW can handle nested transactions.
* Unicode is handled correctly.
* APSW is faster.
"""
import apsw
from peewee import *
from peewee import _sqlite_date_part
from peewee import _sqlite_date_trunc
from peewee import _sqlite_regexp
from peewee import BooleanField as _BooleanField
from peewee import DateField as _DateField
from peewee import DateTimeField as _DateTimeField
from peewee import DecimalField as _DecimalField
from peewee import logger
from peewee import savepoint
from peewee import TimeField as _TimeField
from peewee import transaction as _transaction
from playhouse.sqlite_ext import SqliteExtDatabase
from playhouse.sqlite_ext import VirtualCharField
from playhouse.sqlite_ext import VirtualField
from playhouse.sqlite_ext import VirtualFloatField
from playhouse.sqlite_ext import VirtualIntegerField
from playhouse.sqlite_ext import VirtualModel


class transaction(_transaction):
    def __init__(self, db, lock_type='deferred'):
        self.db = db
        self.lock_type = lock_type

    def _begin(self):
        self.db.begin(self.lock_type)


class APSWDatabase(SqliteExtDatabase):
    def __init__(self, database, timeout=None, **kwargs):
        self.timeout = timeout
        self._modules = {}
        super(APSWDatabase, self).__init__(database, **kwargs)

    def register_module(self, mod_name, mod_inst):
        self._modules[mod_name] = mod_inst

    def unregister_module(self, mod_name):
        del(self._modules[mod_name])

    def _connect(self, database, **kwargs):
        conn = apsw.Connection(database, **kwargs)
        if self.timeout is not None:
            conn.setbusytimeout(self.timeout)
        conn.createscalarfunction('date_part', _sqlite_date_part, 2)
        conn.createscalarfunction('date_trunc', _sqlite_date_trunc, 2)
        conn.createscalarfunction('regexp', _sqlite_regexp, 2)
        self._load_aggregates(conn)
        self._load_collations(conn)
        self._load_functions(conn)
        self._load_modules(conn)
        return conn

    def _load_modules(self, conn):
        for mod_name, mod_inst in self._modules.items():
            conn.createmodule(mod_name, mod_inst)
        return conn

    def _load_aggregates(self, conn):
        for name, (klass, num_params) in self._aggregates.items():
            def make_aggregate():
                instance = klass()
                return (instance, instance.step, instance.finalize)
            conn.createaggregatefunction(name, make_aggregate)

    def _load_collations(self, conn):
        for name, fn in self._collations.items():
            conn.createcollation(name, fn)

    def _load_functions(self, conn):
        for name, (fn, num_params) in self._functions.items():
            conn.createscalarfunction(name, fn, num_params)

    def _execute_sql(self, cursor, sql, params):
        cursor.execute(sql, params or ())
        return cursor

    def execute_sql(self, sql, params=None, require_commit=True):
        logger.debug((sql, params))
        with self.exception_wrapper():
            cursor = self.get_cursor()
            self._execute_sql(cursor, sql, params)
        return cursor

    def last_insert_id(self, cursor, model):
        return cursor.getconnection().last_insert_rowid()

    def rows_affected(self, cursor):
        return cursor.getconnection().changes()

    def begin(self, lock_type='deferred'):
        self.get_cursor().execute('begin %s;' % lock_type)

    def commit(self):
        self.get_cursor().execute('commit;')

    def rollback(self):
        self.get_cursor().execute('rollback;')

    def transaction(self, lock_type='deferred'):
        return transaction(self, lock_type)

    def savepoint(self, sid=None):
        return savepoint(self, sid)


class TableFunction(object):
    columns = None
    parameters = None
    name = None

    @classmethod
    def get_columns(cls):
        if cls.columns is None:
            raise ValueError('No columns defined.')
        return cls.columns

    @classmethod
    def get_parameters(self):
        if cls.parameters is None:
            raise ValueError('No parameters defined.')
        return cls.parameters

    def initialize(self, **query):
        raise NotImplementedError

    def iterate(self, idx):
        raise NotImplementedError

    @classmethod
    def module(cls):
        module = type('%sModule' % cls.__name__, (BaseModule,), {})
        return module(cls)


class Cursor(object):
    def __init__(self, table_func):
        self.table_func = table_func
        self.current_row = None
        self._idx = 0
        self._consumed = False

    def Close(self):
        pass

    def Column(self, idx):
        if idx == -1:
            return self._idx
        return self.current_row[idx - 1]

    def Eof(self):
        return self._consumed

    def Filter(self, idx_num, idx_name, constraint_args):
        """
        This method is always called first to initialize an iteration
        to the first row of the table. The arguments come from the
        BestIndex() method in the table object with constraintargs
        being a tuple of the constraints you requested. If you always
        return None in BestIndex then indexnum will be zero,
        indexstring will be None and constraintargs will be empty).
        """
        query = {}
        params = idx_name.split(',')
        for idx, param in enumerate(params):
            value = constraint_args[idx]
            query[param] = value

        self.table_func.initialize(**query)
        self.Next()

    def Next(self):
        try:
            self.current_row = self.table_func.iterate(self._idx)
        except StopIteration:
            self._consumed = True
        else:
            self._idx += 1

    def Rowid(self):
        return self._idx


class Table(object):
    def __init__(self, table_func, params):
        self.table_func = table_func
        self.params = params

    def Open(self):
        return Cursor(self.table_func())

    def Disconnect(self):
        pass

    Destroy = Disconnect

    def UpdateChangeRow(self, *args):
        raise ValueError('Cannot modify eponymous virtual table.')

    UpdateDeleteRow = UpdateInsertRow = UpdateChangeRow

    def BestIndex(self, constraints, order_bys):
        """
        Example query:

        SELECT * FROM redis_tbl
        WHERE parent = 'my-hash' AND type = 'hash';

        Since parent is column 4 and type is colum 3, the constraints
        will be:

        (4, apsw.SQLITE_INDEX_CONSTRAINT_EQ),
        (3, apsw.SQLITE_INDEX_CONSTRAINT_EQ)

        Ordering will be a list of 2-tuples consisting of the column
        index and boolean for descending.

        Return values are:

        * Constraints used, which for each constraint, must be either
          None, an integer (the argument number for the constraints
          passed into the Filter() method), or (int, bool) tuple.
        * Index number (default zero).
        * Index string (default None).
        * Boolean whether output will be in same order as the ordering
          specified.
        * Estimated cost in disk operations.
        """
        constraints_used = []
        columns = []
        for i, (column_idx, comparison) in enumerate(constraints):
            if comparison != apsw.SQLITE_INDEX_CONSTRAINT_EQ:
                continue

            # Instruct SQLite to pass the constraint value to the
            # Cursor's Filter() method.
            constraints_used.append(i)

            # We will generate a string containing the columns being
            # filtered on, otherwise our Cursor won't know which
            # columns the filter values correspond to.
            columns.append(self.params[column_idx - 1])

        return [
            constraints_used,  # Indices of constraints we are
                               # interested in.
            0,  # The index number, not used by us.
            ','.join(columns),  # The index name, a list of filter
                                # columns.
            False,  # Whether the results are ordered.
            1000,
        ]


class BaseModule(object):
    def __init__(self, table_func):
        self.table_func = table_func

    def Connect(self, connection, module_name, db_name, table_name,
                *args):
        # Retrieve the cols and params from the TableFunction.
        columns = self.table_func.get_columns()
        parameters = self.table_func.get_parameters()

        columns = ','.join(
            columns +
            ['%s HIDDEN' % param for param in parameters])

        return ('CREATE TABLE x(%s);' % columns,
                Table(self.table_func, parameters))

    Create = Connect


def nh(s, v):
    if v is not None:
        return str(v)

class BooleanField(_BooleanField):
    def db_value(self, v):
        v = super(BooleanField, self).db_value(v)
        if v is not None:
            return v and 1 or 0

class DateField(_DateField):
    db_value = nh

class TimeField(_TimeField):
    db_value = nh

class DateTimeField(_DateTimeField):
    db_value = nh

class DecimalField(_DecimalField):
    db_value = nh
