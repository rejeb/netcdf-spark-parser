credentials += Credentials(
  "Sonatype Nexus Repository Manager",
  "central.sonatype.com",
  sys.env("REPO_USERNAME"),
  sys.env("REPO_PASSWORD")
)
