Problem Statement: Bittorrent Client
Repo: https://github.com/TatHack-Tathva/torrent-client
Technology: Go, Bencode, Networking

You are given a partially completed BitTorrent client implementation in Go. While the networking and protocol skeleton are present, the codebase contains intentional implementation bugs that break tracker communication, wire protocol serialization, bitfield evaluation, and request pipeline flow control.
Your primary objective is to debug the core protocol stack, establish stable peer-to-peer downloads across standard torrent swarms, and extend the system with innovative feature additions.
Please refer to the README.md for any further clarification regarding the background of this project or setting it up.
Task Overview
Debugging Phase
Find and fix the bugs introduced across the code sections handling info-hash generation, wire protocol message encoding/decoding, bitwise piece availability checks, and worker backlog state management.
Feature Extension
You are expected to implement the following baseline additions:
Live Terminal Progress Dashboard: Replace static log outputs with a dynamic, real-time terminal user interface showing download speed (KB/s or MB/s), estimated completion time (ETA), active worker goroutines, and piece progress.
Download Resuming: Implement state serialization to save downloaded piece states and bitfield maps to disk, allowing interrupted downloads to resume without re-downloading verified pieces.
Seeding / Peer Upload Handler: Open a TCP listener port and implement response handlers for incoming peer handshakes and piece request messages to upload downloaded pieces back to the network.

Feel free to make any modifications, architectural refactors, or feature additions as you please! Additional features you add will be heavily weighted during evaluation.
Ideas for bonus features: Magnet Link (BEP 0009) resolution, UDP tracker support (BEP 0015), multi-file torrent structure parsing, custom bandwidth rate limiting.
