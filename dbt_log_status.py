from dataclasses import dataclass
import re
import argparse
from enum import Enum

# AC:
# works on sample console_output.log
# add actual status of job start -> ok -> completed  
# fn log passed optionally on cmd line
# top_n_dbt_models_status

@dataclass(order=True)
class DBTModelStatus:
    """DBTModelStatus
    dbt models status representation of raw log data
        ex: 05:56:24  5 of 135 START table model schema.some_model ........................ [RUN]
            05:56:36  5 of 135 OK created table model schema.some_model ................... [SELECT in 11.69s]
    _raw
    _status
    some getter() to query the instance status, runtime ad model name
    """
    
    class Status(Enum):
        UNKNOWN = -1
        STARTED = 1
        PASS = 2
        ERROR = 4
        COMPLETED = 3
        COMPLETED_ERROR=5

    _raw: str = ""
    _status: Status = Status.UNKNOWN

    def __init__(self,raw) -> None:
        self._raw = raw
        self._status = DBTModelStatus.Status.UNKNOWN
        res = re.search(r'(\d*:\d*:\d*\s*\d*\sof\s*\d*)\s*(START|ERROR|OK)',self._raw)
        if res:
            if res.group(2)=='START':
                self._status = DBTModelStatus.Status.STARTED
            if res.group(2)=='OK':
                self._status =DBTModelStatus.Status.PASS
            if res.group(2)=='ERROR':
                self._status =DBTModelStatus.Status.ERROR

    def get_status(self):
        return self._status

    def get_runtime(self):
        """get runtime 
        expected as 
            ex: [SELECT in 9.40s]
                9.40
        """
        res=re.search(r'(\sin\s[0-9.]*s[]])',self._raw)
        if res:
            in_sec_str =  '['+ res.group(1)  
            res_secs=re.search(r'([0-9.]*s)',in_sec_str)
            runtime_txt = res_secs.group(1).rstrip('s')
            res = float(runtime_txt)
            return res

    def get_id(self):
        """get id 
            ex: 05:56:35  7 of 135 
                7
        """
        res = re.search(r'(\d*:\d*:\d*)\s*(\d*\s(of)\s*\d*)',self._raw)
        if res:
            d_of_d_txt= res.group(2)
            padded = d_of_d_txt[:d_of_d_txt.index(' of ')]
            return int(padded.lstrip().rstrip())

    def get_model(self):
        """get model
            ex: 9 of 135 START table model schema.some_model
                schema.some_model
        """
        res=re.search(r'(\d*:\d*:\d*\s*\d*\sof\s*\d*\s*)(OK|START|ERROR)\s([.a-z_0-9 ]*)',self._raw)
        if res:
            return res.group(3).removesuffix('.')

    @classmethod
    def is_a_model_line(cls,txt):
        """as only lines with 
            hh:mm:ss n of m 
            entries are useful in our case
        """
        return re.match(r'(\d*:\d*:\d*)\s*(\d*\s(of)\s*\d*)',txt)
    
    @classmethod
    def read_log_models_lines_only(cls, file_name):
        """Read raw txt (only valid lines related to models)
        - dbt use crappy ansi color code 
            ex [0m0 we dont need them here
        - save cleanup log as txt as extra
        """

        def escape_ansi(line):
            # https://stackoverflow.com/questions/14693701/how-can-i-remove-the-ansi-escape-sequences-from-a-string-in-python
            ansi_escape = re.compile(r'(?:\x1B[@-_]|[\x80-\x9F])[0-?]*[ -/]*[@-~]')
            return ansi_escape.sub('', line)

        lines = [escape_ansi(line) for line in open(file_name, "r") ]
        is_a_model_lines = list(filter(lambda m: DBTModelStatus.is_a_model_line(m), lines))
        with open('printable_'+file_name, 'w') as textfile:
            textfile.writelines(str(i) for i in lines)
        return is_a_model_lines

    @classmethod
    def read_log_as_dbt_models_status(cls, printable_lines):
        """raw text as data class
        """
        dbl_models_status = [DBTModelStatus(raw = printable_line) for printable_line in printable_lines ] 
        return dbl_models_status

    @classmethod
    def consolidate_dbt_models_status(cls, dbt_models_status):
        """multiple lines for the same models are consolidated as one
            ex START OK => COMPLETED
        """
        consolidated_dbt_models_status = {}
        for m in dbt_models_status:
            m_found = consolidated_dbt_models_status.get(m.get_id(), None)
            if not m_found:
                consolidated_dbt_models_status[m.get_id()] = m
            else:
                if m_found.get_status() == DBTModelStatus.Status.STARTED and m._status == DBTModelStatus.Status.PASS:
                    m._status =DBTModelStatus.Status.COMPLETED
                if m_found.get_status() == DBTModelStatus.Status.STARTED and m._status == DBTModelStatus.Status.ERROR:
                    m._status =DBTModelStatus.Status.COMPLETED_ERROR
                consolidated_dbt_models_status[m.get_id()] = m
        return consolidated_dbt_models_status

    @classmethod
    def top_n_runtime_dbt_models_status(cls, consolidated_dbt_models_status_dict,top_n):
        """get top n by most runtime
        """
        consolidated_dbt_models_status = [v for k,v in consolidated_dbt_models_status_dict.items()]
        consolidated_dbt_models_status_tuples = [(
            0 if v.get_id() is None else v.get_id(),
            "???" if v.get_model() is None else v.get_model(),
            0 if v.get_runtime() is None else v.get_runtime()
            ) for v in consolidated_dbt_models_status]
        # sorted by desc runtime
        sorted_consolidated_dbt_models_status_tuples= sorted(consolidated_dbt_models_status_tuples, key=lambda tup: (tup[2]), reverse=True )
        return sorted_consolidated_dbt_models_status_tuples[1:top_n]


    def __repr__(self):
        res = "DBTModelStatus:"
        res+=f'\tid: {self.get_id()}'
        res+=f'\tmodel: {self.get_model()}' 
        res+=f'\tstatus: {self.get_status()}' 
        res+=f'\truntime: {self.get_runtime()}'
        return res

    def __str__(self) -> str:
        res = self.__repr__()
        res+=f'\traw: {self._raw[:]}'
        return res

def main():

    def _get_log_file_name():
        parser = argparse.ArgumentParser()
        parser.add_argument('--dbt_log', dest='dbt_log', type=str, help='dbt_log fn')
        args = parser.parse_args()
        if not args.dbt_log:
            return "console_output.log"
        else:   
            return args.dbt_log

    print("\n*** read_log_as_printable_lines") 
    log_file_name=_get_log_file_name()
    printable_lines = DBTModelStatus.read_log_models_lines_only(log_file_name)
    for v in printable_lines: print(v)
    
    print("\n*** read_log_as_dbt_models_status")    
    dbt_models_status = DBTModelStatus.read_log_as_dbt_models_status(printable_lines)
    for v in dbt_models_status: print(v)
    
    print("\n*** consolidate_dbt_models_status")
    consolidated_dbt_models_status_dict = DBTModelStatus.consolidate_dbt_models_status(dbt_models_status)
    for k,v in consolidated_dbt_models_status_dict.items(): print(v.__repr__())
    
    print("\n*** top_n_runtime_dbt_models_status")
    top_n=10
    top_n_dbt_models_status = DBTModelStatus.top_n_runtime_dbt_models_status(consolidated_dbt_models_status_dict,top_n)
    for v in top_n_dbt_models_status: print(v)

if __name__ == "__main__":
    main()