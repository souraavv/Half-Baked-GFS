# Half-baked-GFS
It is a simplified version of Google File System. It is written in Python and attempts to emulate the overall architecture and design of the original Google File System while also maintaining a level of simplicity for both client use and programmer development. It allows a client to create, read, write, delete and restore files within a distributed filesystem while also allowing for recovery from chunkserver failures.

## Master

Master manages the entire control flow of the system. It acts as an initiator of the communication between the client and the chunkservers. It handles the creation, removal and management of chunks and files and ensures the correct behaviour of the system.

``` python master.py <master_url> <master_port> ```

## Chunkserver

Chunkserver manages the storage of chunks. It handles read and write requests from the client.

``` python chunkserver.py <chunkserver_url> <chunkserver_port> <master_url> <master_port> <storage_dir> ```

## Client

Client will be the endpoint available for the end applications to perform operations on stored files in Half Baked GFS.

``` python client.py <client_url> <client_port> <master_url> <master_port> ```

## Test script for client

Test script simulates end application which makes use of client to perform operations over the files stored in Half Baked GFS.

Fill IP1, IP2, IP3 in IPS list which corresponds to ip addresses of the chunkservers.
``` python test_client.py <client_url> <client_port> ```

## Links
- [Report](https://drive.google.com/file/d/1CHr3GkEUfWA1RGMA_tm6kvjbv4dYSTW0/view?usp=sharing)
- [Presentation](https://drive.google.com/file/d/10_SOlnbLlE6LVK6HJgH3xj9b5yZJcxM7/view?usp=sharing)
- [Slides](https://docs.google.com/presentation/d/1Fv_nA9KA5Q7Z6OzjOObs98Ojo8lNeO_xQnQr_C3dUMQ/edit?usp=sharing)

## References
- [The Google File System (SOSP, 2003)](https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf)