#!/Library/Frameworks/Python.framework/Versions/3.9/bin/python3

import shutil
import time
import os

BEGIN = "BEGIN"
WRITE = "WRITE"
READ = "READ"
COMMIT = "COMMIT"
ERROR = "ERROR"
MOVE = "mov"
WITHDRAW = "wit"
DEPOSIT = "dep"
ID_SEPARATOR = "_"
NEWLINE = "\n"

class DataStore:
    def __init__(self, path):
        self.path = path
        if not os.path.exists(path):
            with open(path, 'w') as _:
                pass

    def log(self, line):
        with open(self.path, "a") as f:
            f.write(line.line())

    def backup_path(self):
        return self.path + "_backup"
    
    def recover(self):
        with open(self.path, "r") as f:
            lines = f.readlines()

        # Assume linear execution. I.e.
        #
        # BEGIN id2
        # L1
        # L2
        # BEGIN id2
        # ...
        # Is an impossible state, since there is a second BEGIN
        # before the first BEGIN's COMMIT. In otherwise, only the last transaction
        # in the file may ever be unfinished.
        # 
        # Therefore, to replay, find the last transaction. Then, if it has a COMMIT
        # do nothing. Else, delete all lines after the BEGIN and replay it.

        # Find last begin.
        last_begin_idx = None
        for idx, line in enumerate(lines):
            if line.startswith(BEGIN):
                last_begin_idx = idx

        if last_begin_idx == None:
            # Empty file.
            return
        
        # Truncate at last begin, but include it.
        lines = lines[:last_begin_idx+1]

        # ID_SEPARATOR our truncated file to a backup location.
        # Then, atomically move the backup location to the original location.
        with open(self.backup_path(), "w") as f:
            f.writelines([line+NEWLINE for line in lines])
        shutil.move(self.backup_path(), self.path)

        # Replay the unfinished BEGIN
        oid = lines[last_begin_idx].split()[1]
        op = self.parse_op_from_id(oid)
        op.handle(write_begin=False)

    def parse_op_from_id(self, oid):
        if oid.startswith(MOVE):
            return MoveOp(self, *oid.split(ID_SEPARATOR)[1:])
        if oid.startswith(WITHDRAW):
            return WithdrawOp(self, *oid.split(ID_SEPARATOR)[1:])
        if oid.startswith(DEPOSIT):
            return DepositOp(self, *oid.split(ID_SEPARATOR)[1:])
        raise ValueError(oid)
    
    def read(self, dst):
        with open(self.path, "r") as f:
            lines = f.readlines()
        for line in reversed(lines):
            if line.startswith(WRITE):
                _, account, amt = line.split()
                if account == dst:
                    return int(amt)
        return 0
    
    def already_committed(self, op):
        with open(self.path, "r") as f:
            lines = f.readlines()
        found_begin_with_oid = False
        for line in lines:
            line = line.strip()
            if op.id() in line:
                found_begin_with_oid = True
            if found_begin_with_oid and line == COMMIT:
                return True
        return False

class BeginLogLine:
    def __init__(self, oid):
        self.preamble = BEGIN
        self.oid = oid
    
    def line(self):
        return f"{self.preamble} {self.oid}\n"

class WriteLogLine:
    def __init__(self, dst, amt):
        self.preamble = WRITE
        self.dst = dst
        self.amt = amt
    
    def line(self):
        return f"{self.preamble} {self.dst} {str(self.amt)}\n"

class ReadLogLine:
    def __init__(self, dst, amt):
        self.preamble = READ
        self.dst = dst
        self.amt = amt
    
    def line(self):
        return f"{self.preamble} {self.dst} {str(self.amt)}\n"
    
class CommitLogLine:
    def __init__(self, err=None):
        self.preamble = COMMIT
        self.err = err
    
    def line(self):
        return self.preamble + ("" if self.err == None else f" {self.err}") + "\n"

class MoveOp:
    def __init__(self, ds, ts, src, dst, amt):
        self.ds = ds
        self.ts = ts
        self.src = src
        self.dst = dst
        self.amt = int(amt)
    
    def id(self):
        return ID_SEPARATOR.join([str(x) for x in [MOVE, self.ts, self.src, self.dst, str(self.amt)]])
    
    def handle(self, write_begin=True):
        if self.ds.already_committed(self):
            return 
        if write_begin:
            self.ds.log(BeginLogLine(self.id()))

        curr = self.ds.read(self.src)
        self.ds.log(ReadLogLine(self.src, curr))
        if curr < self.amt:
            self.ds.log(CommitLogLine(f"{ERROR}: insufficient funds: {curr} < {self.amt}"))
            return
        new = curr - self.amt
        self.ds.log(WriteLogLine(self.src, new))

        curr = self.ds.read(self.dst)
        self.ds.log(ReadLogLine(self.dst, curr))
        new = curr + self.amt
        self.ds.log(WriteLogLine(self.dst, new))

        self.ds.log(CommitLogLine())
    
class WithdrawOp:
    def __init__(self, ds, ts, dst, amt):
        self.ds = ds
        self.ts = ts
        self.dst = dst
        self.amt = int(amt)

    def id(self):
        return ID_SEPARATOR.join([str(x) for x in [WITHDRAW, self.ts, self.dst, str(self.amt)]])
    
    def handle(self, write_begin=True):
        if self.ds.already_committed(self):
            return
        if write_begin:
            self.ds.log(BeginLogLine(self.id()))
        curr = self.ds.read(self.dst)
        self.ds.log(ReadLogLine(self.dst, curr))
        if curr < self.amt:
            self.ds.log(CommitLogLine(f"{ERROR}: insufficient funds: {curr} < {self.amt}"))
            return
        new = curr - self.amt
        self.ds.log(WriteLogLine(self.dst, new))
        self.ds.log(CommitLogLine())

class DepositOp:
    def __init__(self, ds, ts, dst, amt):
        self.ds = ds
        self.ts = ts
        self.dst = dst
        self.amt = int(amt)

    def id(self):
        return ID_SEPARATOR.join([str(x) for x in [DEPOSIT, self.ts, self.dst, str(self.amt)]])

    def handle(self, write_begin=True):
        if self.ds.already_committed(self):
            return
        if write_begin:
            self.ds.log(BeginLogLine(self.id()))
        curr = self.ds.read(self.dst)
        self.ds.log(ReadLogLine(self.dst, curr))
        new = curr + self.amt
        self.ds.log(WriteLogLine(self.dst, new))
        self.ds.log(CommitLogLine())

ds = DataStore("/Users/afichman/Desktop.nosync/Projects/idempotency-example/store")
ops = [
    DepositOp(ds, time.time_ns(), "A", 5),
    DepositOp(ds, time.time_ns(), "A", 10),
    DepositOp(ds, time.time_ns(), "A", 5),
]
for op in ops:
    op.handle()