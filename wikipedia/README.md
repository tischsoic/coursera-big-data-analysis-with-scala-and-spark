In case of java.net.UnknownHostException while building project in IntelliJ:

https://github.com/sbt/sbt/issues/4103#issuecomment-530140535
http://www.scalatest.org/supersafe#installation
```
# 1. Add the Artima Maven Repository as a resolver in ~/.sbt/0.13/global.sbt, like this:

resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"
```