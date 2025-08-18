# Modnet: like freenet, but for mods.

NexusMods is hosted in the UK where police will berate you for even something as simple as "catcalling", this means that a large portion of potential mods will be banned due to government censorship.

Additionally, all current mod platforms are beholden to ads or user payments. This means that even if the government doesnt complain, you may not be able to afford to keep the servers online or replace a drive if CCs and ad agencies dont like you.

The alternative: host mods in chunks on thousands of computers, or potentially in the future, millions (doubtful though. we might reach 10k at peak). Then, if government kills 1 computer, there are many still hosting the mod.

The major concerns: what if someone uploads something illegal for me to host? what if I like censoring? what if I cant host but a small amount?

The answer to 1: you can block certain mods based on their hash (or will be able to by the time this is "complete"), or you can block uploaders based on their UUID, or foreign hosts based on their UUID.

The answer to 2: you can work for the government.

The answer to 3: for now, create a partition. Eventually, this will have proper size limits and rate limits implemented.


To Run: 
## Option 1:
Install Python
```
git clone https://github.com/yggdrasil75/skyrim-modnet
python -m venv venv
source venv/bin/activate OR venv\scripts\activate
pip install -r requirements.txt
python backend/app.py
```
## option 2:
Install Docker

```
git clone https://github.com/yggdrasil75/skyrim-modnet
docker compose up -d
```
Current Version: 0.0.1.
In this version, its a stupidly simple site with no content. you can tell me your public IP and I can link up or I can send you mine. Its not really any more secure than us sending over any random P2P file sharing app (mutorrent or libtorrent for instance), but it will show all files known to any given host.
