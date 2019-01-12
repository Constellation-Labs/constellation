### On design choices

This file summarizes current resoning for our approach and design decisions, 
reviews and references. 

Many decissions can be traced back to the fundamental goal to provide and
accessible, scalable protocol that focuses on solving the consensus task. 
For a cursory glipse into the core teams perspectives, you may check out the following clips:

* [youtube.com/constellation-labs/talk-at-Tech-Crunch](https://youtu.be/fCscJL3_tdU) (Oct. 2018, 28 mins)
* [youtube.com/constellation-labs/testnet-overview](https://youtu.be/SsYZF4msXuQ) (Aug. 2018, 22 mins)
  
#### Why scala?
There are some general notes on Scala and also on other functional programming languages actively used for crypto projects in the
[/wiki/Comparisons-to-other-protocols](https://github.com/Constellation-Labs/constellation/wiki/Comparisons-to-other-protocols#fast_forward-projects-using-a-functional-language-approach).

One motivating factor as language of choice for the reference implementation of the protocol was of course the core teams experience with it, as well as the useful packages like akka actors and apache spark on the Java virtual machine (JVM). The constellation code base also makes extensive use of the type hierarchy features. In fact, the para-protocol approach to dApp integration builds on it.

#### On architecture

For diagrams, see 
[/docs/architecture.md](https://github.com/Constellation-Labs/constellation/blob/dev/docs/architecture.md).

#### Feedback

Please communicate suggestions by making a thread on the 
[community portal Orion](https://orion.constellationlabs.io/accounts/login/?next=/) 
or approaching the developers on the 
[discord](https://discordapp.com/invite/KMSmXbV) 
server:

  <a href="https://discordapp.com/invite/KMSmXbV">
	  <img src="https://img.shields.io/badge/chat-discord-brightgreen.svg"/>
  </a>
