// To sync with Maven central, you need to supply the following information:
publishMavenStyle := true

licenses := Seq("APL2" -> url("https://github.com/izhangzhihao/soozie/blob/master/LICENSE"))
homepage := Some(url("https://github.com/izhangzhihao/soozie"))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/izhangzhihao/soozie"),
    "scm:git@github.com:izhangzhihao/soozie.git"
  )
)
developers := List(
  Developer(id="izhangzhihao", name="张志豪", email="izhangzhihao@hotmail.com", url=url("https://github.com/izhangzhihao"))
)

publishTo in ThisBuild := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)