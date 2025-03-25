'''
Read/Search (ie. SELECT) methods for TDLB.Adaptor 

Steve Mathias
smathias@salud.unm.edu
Time-stamp: <2025-02-13 16:17:46 smathias>
'''
from contextlib import closing
from collections import defaultdict
import logging

class ReadMethodsMixin:
  def get_target_ids(self):
    '''
    Function  : Get all TDLBase target ids
    Arguments : N/A
    Returns   : A list of integers
    Scope     : Public
    '''
    sql = "SELECT id FROM target"
    with closing(self._conn.cursor()) as curs:
      curs.execute(sql)
      ids = [row[0] for row in curs.fetchall()]
    return ids

  def find_target_ids(self, q, incl_alias=False):
    '''
    Function  : Find id(s) of target(s) that satisfy the input query criteria
    Arguments : A dictionary containing query criteria and an optional boolean flag
    Returns   : A list of integers, or False if no targets are found
    Examples  : Search by HGNC Gene Symbol:
                target_ids = dba.find_target_ids({'sym': 'CHERP'})
                Search by UniProt Accession, including aliases:
                target_ids = dba.find_target_ids({'uniprot': 'O00302'}, incl_alias=True)
                Search by name (ie. Swissprot Accession):
                target_ids = dba.find_target_ids({'name': '5HT1A_HUMAN'})
                Search by NCBI Gene ID:
                target_ids = dba.find_target_ids({'geneid': '167359'})
                Search by STRING ID:
                target_ids = dba.find_target_ids({'stringid': 'ENSP00000300161'})
    Scope     : Public
    Comments  : The incl_alias flag only works for symbol and uniprot queries, as these are the only identifier types in the alias table.
    '''
    sql ="SELECT id FROM target t WHERE "
    if 'sym' in q:
      if incl_alias:
        sql += " t.sym = %s UNION SELECT target_id FROM alias WHERE atype = 'symbol' AND value = %s"
        params = (q['sym'], q['sym'])
      else:
        sql += " t.sym = %s"
        params = (q['sym'],)
    elif 'uniprot' in q:
      if incl_alias:
        sql += " t.uniprot = %s UNION SELECT target_id FROM alias WHERE atype = 'uniprot' AND value = %s"
        params = (q['uniprot'], q['uniprot'])
      else:
        sql += " t.uniprot = %s"
        params = (q['uniprot'],)
    elif 'name' in q:
      sql += " t.name = %s"
      params = (q['name'],)
    elif 'geneid' in q:
      sql += " t.geneid = %s"
      params = (q['geneid'],)
    elif 'stringid' in q:
      sql += " t.stringid = %s"
      params = (q['stringid'],)
    else:
      self.warning("Invalid query parameters sent to find_target_ids(): ", q)
      return False
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
  
    ids = []
    with closing(self._conn.cursor()) as curs:
      curs.execute(sql, params)
      ids = [row[0] for row in curs.fetchall()]
    return ids

  def find_target_ids_by_xref(self, q):
    '''
    Function  : Find id(s) of target(s) that satisfy the input query criteria of xref type and value
    Arguments : A distionary containing query criteria
    Returns   : A list of integers, or False if no targets are found
    Examples  : Find target(s) by RefSeq xref:
                tids = dba.find_target_ids_by_xref({'xtype': 'RefSeq', 'value': 'NM_123456'})
    Scope     : Public
    '''
    if 'xtype' not in q or 'value' not in q:
      self.warning(f"Invalid query parameters sent to find_target_ids_by_xref(): {q}")
      return False

    ids = []
    sql = "SELECT target_id FROM xref WHERE xtype = %s AND value = %s"
    params = (q['xtype'], q['value'])
    with closing(self._conn.cursor()) as curs:
      curs.execute(sql, params)
      ids = [row[0] for row in curs.fetchall()]
    return ids

  def get_target(self, id, annot=False):
    '''
    Function  : Get target data by id
    Arguments : An integer and an optional boolean
    Returns   : Dictionary containing target data
    Example   : target = dba->get_target(42, annot=True) 
    Scope     : Public
    Comments  : By default, this returns only data in the target table.
                To get all associated annotations, call with
                annot=True.
    '''
    with closing(self._conn.cursor(dictionary=True)) as curs:
      self._logger.debug("ID: %s" % id)
      curs.execute("SELECT * FROM target WHERE id = %s", (id,))
      t = curs.fetchone()
      if not t: return False
      if annot:
        # tdl_info
        t['tdl_infos'] = {}
        curs.execute("SELECT * FROM tdl_info WHERE target_id = %s", (id,))
        for ti in curs:
          self._logger.debug("  tdl_info: %s" % str(ti))
          itype = ti['itype']
          val_col = self._info_types[itype]
          t['tdl_infos'][itype] = {'id': ti['id'], 'value': ti[val_col]}
        if not t['tdl_infos']: del(t['tdl_infos'])
        # aliases
        t['aliases'] = []
        curs.execute("SELECT * FROM alias WHERE target_id = %s", (id,))
        for a in curs:
          t['aliases'].append(a)
        if not t['aliases']: del(t['aliases'])
        # xrefs
        t['xrefs'] = {}
        for xt in self._xref_types:
          l = []
          curs.execute("SELECT * FROM xref WHERE target_id = %s AND xtype = %s", (id, xt))
          for x in curs:
            init = {'id': x['id'], 'value': x['value']}
            if x['xtra']:
              init['xtra'] = x['xtra']
            l.append(init)
          if l:
            t['xrefs'][xt] = l
        if not t['xrefs']: del(t['xrefs'])
        # Drug Activity
        t['drug_activities'] = []
        curs.execute("SELECT * FROM drug_activity WHERE target_id = %s", (id,))
        for da in curs:
          t['drug_activities'].append(da)
        if not t['drug_activities']: del(t['drug_activities'])
        # Cmpd Activity
        t['cmpd_activities'] = []
        curs.execute("SELECT * FROM cmpd_activity WHERE target_id = %s", (id,))
        for ca in curs:
          t['cmpd_activities'].append(ca)
        if not t['cmpd_activities']: del(t['cmpd_activities'])
        # generifs
        t['generifs'] = []
        curs.execute("SELECT * FROM generif WHERE target_id = %s", (id,))
        for gr in curs:
          p['generifs'].append({'id': gr['id'], 'pubmed_ids': gr['pubmed_ids'], 'text': gr['text']})
        if not t['generifs']: del(t['generifs'])
        # goas
        t['goas'] = []
        curs.execute("SELECT * FROM goa WHERE target_id = %s", (id,))
        for g in curs:
          t['goas'].append(g)
        if not t['goas']: del(t['goas'])
        # pmscores
        t['pmscores'] = []
        curs.execute("SELECT * FROM pmscore WHERE target_id = %s", (id,))
        for pms in curs:
          t['pmscores'].append(pms)
        if not t['pmscores']: del(t['pmscores'])
      return t
  
  def get_target4tdlcalc(self, id):
    '''
    Function  : Get a target and associated data required for TDL calculation
    Arguments : An integer
    Returns   : Dictionary containing target data.
    Scope     : Public
    '''
    with closing(self._conn.cursor(dictionary=True, buffered=True)) as curs:
      self._logger.debug("ID: %s" % id)
      curs.execute("SELECT * FROM target WHERE id = %s", (id,))
      t = curs.fetchone()
      if not t: return False
      # Drug Activities
      t['drug_activities'] = []
      curs.execute("SELECT * FROM drug_activity WHERE target_id = %s", (id,))
      for da in curs:
        t['drug_activities'].append(da)
      if not t['drug_activities']: del(t['drug_activities'])
      # Cmpd Activity
      t['cmpd_activities'] = []
      curs.execute("SELECT * FROM cmpd_activity WHERE target_id = %s", (id,))
      for ca in curs:
        t['cmpd_activities'].append(ca)
      if not t['cmpd_activities']: del(t['cmpd_activities'])
      curs.execute("SELECT * FROM tdl_info WHERE itype = 'JensenLab PubMed Score' AND target_id = %s", (id,))
      pms = curs.fetchone()
      t['tdl_infos']['JensenLab PubMed Score'] = {'id': pms['id'], 'value': str(pms['number_value'])}
      t['efl_goas'] = []
      curs.execute("SELECT * FROM tdl_info WHERE itype = 'Experimental MF/BP Leaf Term GOA' AND target_id = %s", (id,))
      for goa in curs:
        t['efl_goas'].append(goa)
      if not t['efl_goas']: del(t['efl_goas'])
      curs.execute("SELECT * FROM tdl_info WHERE itype = 'Ab Count' AND target_id = %s", (id,))
      abct = curs.fetchone()
      t['tdl_infos']['Ab Count'] = {'id': abct['id'], 'value': str(abct['integer_value'])}
      t['generifs'] = []
      curs.execute("SELECT * FROM generif WHERE target_id = %s", (id,))
      for gr in curs:
        t['generifs'].append({'id': gr['id'], 'pubmed_ids': gr['pubmed_ids'], 'text': gr['text']})
      if not t['generifs']: del(t['generifs'])
    return t

  def get_domain_xrefs(self, id):
    '''
    Function  : Get Pfam, InterPro and PROSITE
    Arguments : A target id
    Returns   : A dictionary of lists of dictionaries
    Scope     : Public
    '''
    xrefs = {}
    sql = "SELECT value, xtra FROM xref WHERE target_id = %s AND xtype = %s"
    with closing(self._conn.cursor(dictionary=True)) as curs:
      for xt in ['Pfam', 'InterPro', 'PROSITE']:
        l = []
        curs.execute(sql, (id, xt))
        xrefs[xt] = [d for d in curs.fetchall()]
    return xrefs

  def get_cmpd_activities(self, catype=None):
    cmpd_activities = []
    sql = "SELECT * FROM cmpd_activity"
    if catype:
      sql += " WHERE catype = '%s'" % catype
    with closing(self._conn.cursor(dictionary=True)) as curs:
      curs.execute(sql)
      cmpd_activities = [row for row in curs.fetchall()]
    return cmpd_activities

  def get_drug_activities(self):
    drug_activities = []
    with closing(self._conn.cursor(dictionary=True)) as curs:
      curs.execute("SELECT * FROM drug_activity")
      drug_activities = [row for row in curs.fetchall()]
    return drug_activities
