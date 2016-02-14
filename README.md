# *Scaflow*

*Scaflow* - new workflow engine for scientific computations, focused on:
- Error handling
- Failure recovery
- Concurrency and scalability
- Ease of use

*Scaflow* was created as a result of research on the actor concurrency model for designing scientific workflows. 
Written entirely in Scala using Akka toolkit.

*Scaflow* mantra:
- Simplicity (Simple API, simple code) couse we believe that simplicity is the key in modern software development
- Use the right tool for the job 

##Getting started

Build the *Scaflow* project:

```
$ sbt publishLocal
```

Then attach to your project:

```
resolvers += "Local Ivy Repository" at Path.userHome.asFile.toURI.toURL + ".ivy2/local"

libraryDependencies += "pl.liosedhel" %% "scaflow" % "1.0-SNAPSHOT"
```

And create your first workflow:

```Scala
implicit val actorSystem = ActorSystem("firstScientificWorkflow")

StandardWorkflow.source(List(1, 2, 3))
        .map(a => a * a)
        .sink(println(_))
        .run
```       

And that's it! Please explore tests for more advanced usage examples.
