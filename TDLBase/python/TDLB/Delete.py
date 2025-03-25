'''
Delete methods for TDLB.Adaptor 

Steve Mathias
smathias@salud.unm.edu
Time-stamp: <2025-02-12 12:39:39 smathias>
'''
from mysql.connector import Error
from contextlib import closing

class DeleteMethodsMixin:

  def del_all_rows(self, table_name):
    if not table_name:
      self.warning("No table name sent to del_all_rows()")
      return False
    dsql = f"DELETE FROM {table_name}"
    asql = f"ALTER TABLE {table_name} AUTO_INCREMENT = 1"
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(dsql)
        row_ct = curs.rowcount
        curs.execute(asql)
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL Error in del_all_rows() for table {table_name}: {e}")
        self._conn.rollback()
        return False
    return row_ct

  def del_tdl_infos(self, itype):
    if not itype:
      self.warning("No itype sent to del_tdl_infos()")
      return False
    sql = f"DELETE FROM tdl_info WHERE itype = %s"
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, (itype,))
        self._conn.commit()
        row_ct = curs.rowcount
      except Error as e:
        self._logger.error(f"MySQL Error in del_tdl_infos() for itype {itype}: {e}")
        self._conn.rollback()
        return False
    return row_ct

  def del_cmpd_activities(self, catype):
    if not catype:
      self.warning("No catype sent to del_cmpd_activities()")
      return False
    sql = f"DELETE FROM cmpd_activity WHERE catype = %s"
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, (catype,))
        self._conn.commit()
        row_ct = curs.rowcount
      except Error as e:
        self._logger.error(f"MySQL Error in del_cmpd_activities() for catype {catype}: {e}")
        self._conn.rollback()
        return False
    return row_ct
