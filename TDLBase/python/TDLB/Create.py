'''
Create (ie. INSERT) methods for TDLB.Adaptor 

Steve Mathias
smathias@salud.unm.edu
Time-stamp: <2025-02-13 13:15:18 smathias>
'''
from mysql.connector import Error
from contextlib import closing

class CreateMethodsMixin:
  
  def ins_target(self, init):
    '''
    Function  : Insert a target and all associated data provided.
    Arguments : Dictionary containing target data.
    Returns   : Integer containing target.id
    Example   : tid = dba->ins_target(init) ;
    Scope     : Public
    Comments  : This only handles data parsed from UniProt XML entries in load-UniProt.py
    '''
    if 'name' in init and 'description' in init and 'uniprot' in init:
      params = [init['name'], init['description'], init['uniprot']]
    else:
      self.warning(f"Invalid parameters sent to ins_target(): {init}")
      return False
    cols = ['name', 'description', 'uniprot']
    vals = ['%s','%s', '%s']
    for optcol in ['up_version', 'geneid', 'sym', 'family', 'chr', 'seq']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])
    sql = "INSERT INTO target (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    target_id = None
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, tuple(params))
        target_id = curs.lastrowid
      except Error as e:
        self._logger.error(f"MySQL Error in ins_target(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    if 'aliases' in init:
      for d in init['aliases']:
        d['target_id'] = target_id
        rv = self.ins_alias(d, commit=False)
        if not rv:
          return False
    if 'xrefs' in init:
      for d in init['xrefs']:
        d['target_id'] = target_id
        rv = self.ins_xref(d, commit=False)
        if not rv:
          return False
    if 'tdl_infos' in init:
      for d in init['tdl_infos']:
        d['target_id'] = target_id
        rv = self.ins_tdl_info(d, commit=False)
        if not rv:
          return False
    if 'goas' in init:
      for d in init['goas']:
        d['target_id'] = target_id
        rv = self.ins_goa(d, commit=False)
        if not rv:
          return False
    try:
      self._conn.commit()
    except Error as e:
      self._conn.rollback()
      self._logger.error(f"MySQL commit error in ins_target(): {e}")
      return False
    return target_id

  def ins_alias(self, init, commit=True):
    if 'target_id' not in init or 'atype' not in init or 'value' not in init:
      self.warning("Invalid parameters sent to ins_alias(): ", init)
      return False
    sql = "INSERT INTO alias (target_id, atype, value) VALUES (%s, %s, %s)"
    params = (init['target_id'], init['atype'], init['value'])
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
      except Error as e:
        self._logger.error(f"MySQL Error in ins_alias(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    if commit:
      try:
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL commit error in ins_alias(): {e}")
        self._conn.rollback()
        return False
    return True

  def ins_xref(self, init, commit=True):
    if 'xtype' in init and 'target_id' in init and 'value' in init:
      params = [init['xtype'], init['target_id'], init['value']]
    else:
      self.warning(f"Invalid parameters sent to ins_xref(): {init}")
      return False
    cols = ['target_id', 'xtype', 'value']
    vals = ['%s','%s','%s']
    if 'xtra' in init:
      cols.append('xtra')
      vals.append('%s')
      params.append(init['xtra'])
    sql = "INSERT INTO xref (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
      except Error as e:
        pass
        # if 'Duplicate entry' in e[1] and "key 'xref_idx3'" in e[1]:
        #   pass
        # else:
        #   self._logger.error(f"MySQL Error in ins_xref(): {e}")
        #   self._logger.error(f"SQLpat: {sql}")
        #   self._logger.error(f"SQLparams: {params}")
        #   self._conn.rollback()
        #   return False
    if commit:
      try:
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL commit error in ins_xref(): {e}")
        self._conn.rollback()
        return False
    return True

  def ins_tdl_info(self, init, commit=True):
    if 'itype' in init:
      itype = init['itype']
    else:
      self.warning(f"Invalid parameters sent to ins_tdl_info(): {init}")
      return False
    if 'string_value' in init:
      val_col = 'string_value'
      value = init['string_value']
    elif 'integer_value' in init:
      val_col = 'integer_value'
      value = init['integer_value']
    elif 'number_value' in init:
      val_col = 'number_value'
      value = init['number_value']
    elif 'boolean_value' in init:
      val_col = 'boolean_value'
      value = init['boolean_value']
    elif 'date_value' in init:
      val_col = 'date_value'
      value = init['date_value']
    else:
      self.warning(f"Invalid parameters sent to ins_tdl_info(): {init}")
      return False
    if 'target_id' in init:
      xid = init['target_id']
      sql = "INSERT INTO tdl_info (target_id, itype, %s)" % val_col
    else:
      self.warning(f"Invalid parameters sent to ins_tdl_info(): {init}")
      return False
    sql += " VALUES (%s, %s, %s)"
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {xid}, {itype}, {value}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, (xid, itype, value))
      except Error as  e:
        self._logger.error(f"MySQL Error in ins_tdl_info(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {xid}, {itype}, {value}")
        self._conn.rollback()
        return False
    if commit:
      try:
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL commit error in ins_tdl_info(): {e}")
        self._conn.rollback()
        return False
    return True

  def ins_generif(self, init, commit=True):
    if 'target_id' in init and 'text' in init:
      params = [init['target_id'], init['text']]
    else:
      self.warning(f"Invalid parameters sent to ins_generif(): {init}")
      return False
    cols = ['target_id', 'text']
    vals = ['%s','%s']
    for optcol in ['pubmed_ids', 'years']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])
    sql = "INSERT INTO generif (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
      except Error as e:
         self._logger.error(f"MySQL Error in ins_generif(): {e}")
         self._logger.error(f"SQLpat: {sql}")
         self._logger.error(f"SQLparams: {params}")
         self._conn.rollback()
         return False
      if commit:
        try:
          self._conn.commit()
        except Error as e:
          self._logger.error(f"MySQL commit error in ins_generif(): {e}")
          self._conn.rollback()
          return False
    return True

  def ins_goa(self, init, commit=True):
    if 'target_id' in init and 'go_id' in init:
      params = [init['target_id'], init['go_id']]
    else:
      self.warning(f"Invalid parameters sent to ins_goa(): {init}")
      return False
    cols = ['target_id', 'go_id']
    vals = ['%s','%s']
    for optcol in ['go_term', 'evidence', 'goeco', 'assigned_by']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])
    sql = "INSERT INTO goa (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
      except Error as e:
         self._logger.error(f"MySQL Error in ins_goa(): {e}")
         self._logger.error(f"SQLpat: {sql}")
         self._logger.error(f"SQLparams: {params}")
         self._conn.rollback()
         return False
      if commit:
        try:
          self._conn.commit()
        except Error as e:
          self._logger.error(f"MySQL commit error in ins_goa(): {e}")
          self._conn.rollback()
          return False
    return True

  def ins_pmscore(self, init, commit=True):
    if 'target_id' in init and 'year' in init and 'score' in init:
      params = [init['target_id'], init['year'], init['score']]
    else:
      self.warning(f"Invalid parameters sent to ins_pmscore(): {init}")
      return False
    sql = "INSERT INTO pmscore (target_id, year, score) VALUES (%s, %s, %s)"
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, tuple(params))
      except Error as e:
        self._logger.error(f"MySQL Error in ins_pmscore(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    if commit:
      try:
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL commit error in ins_pmscore(): {e}")
        self._conn.rollback()
        return False
    return True

  def ins_drug_activity(self, init, commit=True):
    if 'target_id' in init and 'drug' in init and 'dcid' in init and 'has_moa' in init:
      params = [init['target_id'], init['drug'],  init['dcid'], init['has_moa']]
    else:
      self.warning(f"Invalid parameters sent to ins_drug_activity(): {init}")
      return False
    cols = ['target_id', 'drug', 'dcid', 'has_moa']
    vals = ['%s','%s','%s', '%s']
    for optcol in ['act_value', 'act_type', 'action_type', 'source', 'reference', 'smiles', 'cmpd_chemblid', 'cmpd_pubchem_cid', 'nlm_drug_info']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])
    sql = "INSERT INTO drug_activity (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, tuple(params))
        if commit: self._conn.commit()
      except Error as  e:
        self._logger.error(f"MySQL Error in ins_drug_activity(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    return True
  
  def ins_cmpd_activity(self, init, commit=True):
    if 'target_id' in init and 'catype' in init and 'cmpd_id_in_src' in init:
      params = [init['target_id'], init['catype'], init['cmpd_id_in_src']]
    else:
      self.warning(f"Invalid parameters sent to ins_cmpd_activity(): {init}")
      return False
    cols = ['target_id', 'catype', 'cmpd_id_in_src']
    vals = ['%s','%s','%s']
    for optcol in ['cmpd_name_in_src', 'smiles', 'act_value', 'act_type', 'reference', 'pubmed_ids', 'cmpd_pubchem_cid']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])
    sql = "INSERT INTO cmpd_activity (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, tuple(params))
        if commit: self._conn.commit()
      except Error as  e:
        self._logger.error(f"MySQL Error in ins_cmpd_activity(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    return True
  
