"""Run the local release checks and bind their results to the exact source tree."""
import os
import shutil
import subprocess
import sys
import time

from .artifacts import source_manifest, compiled_manifest
from .core import ROOT, atomic_json, check


def main():
    forge=shutil.which("forge") or str(__import__("pathlib").Path.home()/".foundry/bin/forge")
    check(os.path.isfile(forge),"Foundry is required")
    (ROOT/'evidence').mkdir(exist_ok=True)
    with open(ROOT/'evidence/validation-build.log','w') as output:
        build=subprocess.run([forge,'build'],cwd=ROOT,stdout=output,stderr=subprocess.STDOUT)
    check(build.returncode == 0, 'contract build failed; see evidence/validation-build.log')
    manifest=source_manifest();compiled=compiled_manifest();checks=[]
    for name,command in (("python",[sys.executable,"-m","pytest","-q"]),("contracts",[forge,"test","-vv"])):
        with open(ROOT/f"evidence/validation-{name}.log","w") as output:
            result=subprocess.run(command,cwd=ROOT,stdout=output,stderr=subprocess.STDOUT)
        checks.append({"check":name,"exit_code":result.returncode,"log":f"evidence/validation-{name}.log"})
    unchanged=source_manifest()==manifest and compiled_manifest()==compiled
    report={"at":time.time(),"passed":unchanged and all(c["exit_code"]==0 for c in checks),"checks":checks,"source_manifest":manifest,"artifact_manifest":compiled,"source_unchanged_during_checks":unchanged}
    atomic_json(ROOT/"evidence/validation.json",report)
    print({"passed":report["passed"],"evidence":"evidence/validation.json"})
    if not report["passed"]:raise SystemExit(1)


if __name__=="__main__":main()
