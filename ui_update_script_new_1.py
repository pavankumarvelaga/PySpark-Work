import os
import subprocess
import sys

a = os.system("gsutil acl ch -r -u regojln66jdh3l2echmlhfckky@speckle-umbrella-3.iam.gserviceaccount.com:R gs://affine_poc/dbinputfiles/`date +%Y-%m-%d`/")
if(a>0):
    print("Error while updating the tool")
    sys.exit(a)

os.system("gsutil cp gs://affine_poc/lookup_files/comp1gce.pem ./")
os.system("chmod 400 comp1gce.pem")
os.system("ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR -o UserKnownHostsFile=/dev/null -i comp1gce.pem skprccm@104.154.74.151 '~/updatetables.sh '`date +%Y-%m-%d` >> updatelog-`date +%Y-%m-%d`.log")

