from readconfig.read_config import ReadConfig
import pyrfc
import pandas as pd
import re


class RfcConnection:
    def __init__(self):
        self.config = ReadConfig(host="redisconfig")
        self.conn = None
        while self.conn is None:
            try:
                self.conn = pyrfc.Connection(
                    ashost=self.config.get("ahost"),
                    sysnr=self.config.get("sysnr"),
                    client=self.config.get("client"),
                    user=self.config.get("rfcuser")["username"],
                    passwd=self.config.get("rfcuser")["passwd"])
            # except pyrfc._exception.CommunicationError as e:
            #     continue
            except pyrfc.CommunicationError:
                continue
            except Exception as e:
                raise Exception(f"Unhandled Exception: {e}")

    def stfc_connection(self, requtext=None):
        requtext = "" if requtext is None else requtext
        return self.conn.call("STFC_CONNECTION", REQUTEXT=requtext)

    def enqueue_report(self, client=None, table_name=None, username=None):
        username = "" if username is None else username
        table_name = "" if table_name is None else table_name
        client = "*" if client is None else client
        return self.conn.call("ENQUEUE_REPORT", GCLIENT=client, GNAME=table_name, GUNAME=username)

    def th_wpinfo(self):
        return self.conn.call("TH_WPINFO")

    def multi_replace(self, s, l, c, sc=None):
        sc = " " if sc is None else sc
        new_str = ""
        r = None
        tmp = s.split(sc)
        for i in tmp:
            if str(c) in str(i):
                try:
                    r = l.pop(0)
                except:
                    r = "&"
                new_str = new_str + r + " "
            else:
                new_str = new_str + i + " "
        return new_str

    def rfc_get_system_info(self):
        return self.conn.call("RFC_GET_SYSTEM_INFO")

    def remote_query_call(self, usergroup, query, variant):
        return self.conn.call("RSAQ_REMOTE_QUERY_CALL", USERGROUP=usergroup, QUERY=query, VARIANT=variant,
                              DATA_TO_MEMORY="X", EXTERNAL_PRESENTATION="Z")

    def call_query(self, usergroup, query, variant):
        data = self.conn.call("RSAQ_REMOTE_QUERY_CALL", USERGROUP=usergroup, QUERY=query, VARIANT=variant,
                              DATA_TO_MEMORY="X", EXTERNAL_PRESENTATION="Z")
        columns = []
        for i in data['LISTDESC']:
            if i['LID'] == 'G00':
                columns.append(i['FCOL'])
        datastring = ""
        datalist = [list(x.values()) for x in data['LDATA']]
        datastring = datastring.join(str(r) for v in datalist for r in v)
        datastring = datastring.split(';/')[0]
        datastring = datastring.split(';')
        datalist = []
        for i in datastring:
            datalist.append(re.split(',\d\d\d:', i[4:]))
        return pd.DataFrame(data=datalist, columns=columns)

    def get_error_code(self, Language="EN", Msg=None, Area=None, Message=None):
        if Msg is None:
            code_query = "SELECT TEXT FROM T100 " \
                         "WHERE SPRSL = '{0}' " \
                         "AND ARBGB = '{1}' " \
                         "AND MSGNR = '{2}'".format(Language, str(Area), str(Message))
            message_text = self.db_query(q=code_query)
            return message_text
        elif Msg is not None and Area is None and Message is None:
            code_query = "SELECT TEXT FROM T100 " \
                         "WHERE SPRSL = '{0}' " \
                         "AND ARBGB = '{1}' " \
                         "AND MSGNR = '{2}'".format(Language, Msg["MSGID"], Msg["MSGNR"])
            message_text = self.db_query(q=code_query)
            r_list = [Msg["MSGV1"], Msg["MSGV2"], Msg["MSGV3"], Msg["MSGV4"]]
            message_text = self.multi_replace(message_text[0][0], r_list, "&")
            return str("Error: " + Msg["MSGID"] + ":" + Msg["MSGNR"] + "  --> " + message_text)

    def db_query(self, q=None, max_rows=None, offset=None, df=False, headers=False, raw=False):
        max_rows = 1000000 if max_rows is None else max_rows
        offset = 0 if offset is None else offset
        if df:
            try:
                results, _headers = self.sql_query(q, max_rows, offset)
                df = pd.DataFrame(results, columns=_headers)
                return df
            except Exception as e:
                print(e)
                return
        elif headers:
            try:
                results, headers = self.sql_query(q, max_rows, offset)
                return dict(dict(zip(headers, results[0])))
            except Exception as e:
                # print(e)
                return
        elif raw:
            try:
                results, headers = self.sql_query(q, max_rows, offset)
                return results, headers
            except Exception as e:
                # print(e)
                return
        else:
            try:
                results, headers = self.sql_query(q, max_rows, offset)
                return results
            except Exception as e:
                print(e)
                return

    def qry(self, Fields, SQLTable, Where, MaxRows=50, FromRow=0):
        """A function to query SAP with RFC_READ_TABLE"""

        # By default, if you send a blank value for fields, you get all of them
        # Therefore, we add a select all option, to better mimic SQL.
        if Fields[0] == '*':
            Fields = ''
        else:
            Fields = [{'FIELDNAME': x} for x in Fields]  # Notice the format

        # the WHERE part of the query is called "options"
        # options = where
        options = [{'TEXT': x} for x in Where]  # again, notice the format

        # we set a maximum number of rows to return, because it's easy to do and
        # greatly speeds up testing queries.
        rowcount = MaxRows

        # Here is the call to SAP's RFC_READ_TABLE
        tables = self.conn.call("RFC_READ_TABLE", QUERY_TABLE=SQLTable, DELIMITER='|', FIELDS=Fields, OPTIONS=options,
                                ROWCOUNT=MaxRows, ROWSKIPS=FromRow)

        # We split out fields and fields_name to hold the data and the column names
        fields = []
        fields_name = []

        data_fields = tables["DATA"]  # pull the data part of the result set
        data_names = tables["FIELDS"]  # pull the field name part of the result set

        headers = [x['FIELDNAME'] for x in data_names]  # headers extraction
        long_fields = len(data_fields)  # data extraction
        long_names = len(data_names)  # full headers extraction if you want it

        # now parse the data fields into a list
        for line in range(0, long_fields):
            fields.append(data_fields[line]["WA"].strip())

        # for each line, split the list by the '|' separator
        fields = [x.strip().split('|') for x in fields]

        # return the 2D list and the headers
        return fields, headers

    def split_where(self, seg):
        # This magical function splits by spaces when not enclosed in quotes..
        where = seg.split(' ')
        where = [x.replace('@', ' ') for x in where]
        return where

    def select_parse(self, statement):
        statement = " ".join([x.strip('\t') for x in statement.upper().split('\n')])

        if 'WHERE' not in statement:
            statement = statement + ' WHERE '

        regex = re.compile("SELECT(.*)FROM(.*)WHERE(.*)")

        parts = regex.findall(statement)
        parts = parts[0]
        select = [x.strip() for x in parts[0].split(',')]
        frm = parts[1].strip()
        where = parts[2].strip()

        # splits by spaces but ignores quoted string with ''
        PATTERN = re.compile(r"""((?:[^ '"]|'[^']*'|"[^"]*")+)""")
        where = PATTERN.split(where)[1::2]

        cleaned = [select, frm, where]
        return cleaned

    def sql_query(self, statement, MaxRows=0, FromRow=0, to_dict=False):
        statement = self.select_parse(statement)

        results = self.qry(statement[0], statement[1], statement[2], MaxRows, FromRow)
        if to_dict:
            headers = statement[0]
            results2 = []
            for line in results:
                new_line = OrderedDict()
                header_counter = 0
                for field in line:
                    try:
                        new_line[headers[header_counter]] = field.strip()
                        header_counter += 1
                    except Exception as e:
                        new_line[headers[header_counter - 1]] = new_line[headers[header_counter - 1]] + " " + " ".join(
                            line[header_counter:])
                        break

                results2.append(new_line)
            results = results2
        return results


if __name__ == "__main__":
    tmp_conn = RfcConnection()
    print(tmp_conn.rfc_get_system_info())
