#!/usr/bin/env python3
import canine

if __name__ == '__main__':
    print("Provisioning workshop cluster")
    email = canine.utils.get_gcp_username()
    user = email.split('@')[0]
    backend = canine.backends.TransientGCPSlurmBackend(
        'wolf-workshop-{}'.format(user),
        controller_type='n1-standard-2',
        project='broad-getzlab-wolfws-202007'
    )
    backend.start()
    print("Your cluster is now ready for the workshop. Please run")
    print("gcloud compute ssh wolf-workshop-{}-controller --project broad-getzlab-wolfws-202007 \\".format(user))
    print("  --zone us-central1-a -- -L 8888:localhost:8888")
    print("to log in to the cluster.")
