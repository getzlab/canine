#!/usr/bin/env python3
import canine

if __name__ == '__main__':
    print("Preparing image for workshop")
    canine.backends.TransientGCPSlurmBackend.personalize_worker_image(
        project='broad-getzlab-wolfws-202007',
    )
    print("Image saved and ready for workshop")
