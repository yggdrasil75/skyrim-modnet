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
Current Version: 0.0.2.

In this version, its a stupidly simple site. You can port forward port 5000 to receieve connections from a remote host, but you will default to seeing what is hosted on the test system (www.themoddingtree.com:5000)
If someone knows nat traversal, then please help me figure it out.









The full plan:
The idea for a new mod hosting platform:
as a standalone program, or (possibly, maybe not) an extension for mo2, vortex, and any other mod manager that allows extensions, have a node running on your computer using probably 50gb of storage per node (configurable to how much you want to host, given that nexus is on petabytes, might be nice to have a lot more.).
you have friends that are prioritized and bypass other nodes (ie: for your next door neighbor, or if you are hosting multiple nodes in your house, possibly a node on your NAS and a node on your gaming desktop)
you have a blacklist of "enemies" which are nodes you will never download from, and nodes of origin that you will not accept files from nor will appear in search (ie: someone uploads something criminal so you block it)
You have "peers" which are connections between 2 systems where they can find strangers and share files
and finally you have "strangers" which are nodes you dont know at all and that dont know you. they can only see that a file was requested, not where its going.

also can block a particular file if you just dont like that 1 mod.

when you upload a file, it is split into chunks and spread to the network. first upload it will host all of the files on your node, over time it will replicate to nodes randomly prioritized based on (network) distance away from you (ie: I upload, neighbors can get file from me, but the person in europe has terrible download speeds due to distance, so send to someone over there in europe, someone in china, someone in japan, someone in brazil, and now those foreigners will get similar download speeds to someone in virginia from me.)
file chunks will have a default of 10 replications, with a minimum default of 3.
after some amount of time, the files will no longer have a guaranteed replication, allowing them to "fall off" the network. when a new node downloads it though, its stored on the node as a new host so that it doesnt disappear. in this way unless we can get to petabytes level of storage, old mods that nobody cares about will disappear while old mods people love will remain.

when you upload a file, you are given a randomly generated password to use to edit it. you can then update the mod files (which will propogate over time across the network), update the page, images, etc. or remove the mod if you want to (which might happen, will rely on nodes to respect the removal request.)

thats general management though, so lets look at modding specific topics for this:
the mod will be extracted and automatically tagged for certain file contents. ie: contains textures if it has dds, meshes if nif, plugins with esp/l/m, and skse with dll.
exe will be blocked from propagating to the network. (sorry, I dont want to have to deal with viruses), and you will be asked to host exes on external platforms with a link on the mod page containing the rest of the files.
dlls can contain viruses, but I dont know how to deal with that effectively without blocking a large portion of mods. I might allow node hosts to set content filters such as "block dlls", maybe have a way to link your own virustotal and scan them.
for game plugins, I may eventually have something like what mators old mod analyzer did so you can check out a mod before downloading it.
it might be possible to set up a custom yolo-style autotagger for some generic and limited scope tags. but dont know for sure.

general searching hopefuls that will be lower priority:
similar mod tagging to nexus where users vote for tags, but instead of 0-99 its -int.max to +int.max (dont know how this will propagate effectively)
new tags can be suggested and when enough mods have the tag suggested, then it shows up as something votable on all pages.
potentially allow nodes to set up their own automation for certain things (ie: tag a mod based on the images of it using a general image tagger)
that last note will be very low priority if it is done.
the idea is that you are the moderator for your own node. you block origins you dont like and allow origins you do. you dont have someone else blocking content that is fully legal just because they take offense to it. because nobody is paying for a handful of expensive large capacity webservers, the site doesnt need funding so CC companies and advertisers cant try to moderate it.
