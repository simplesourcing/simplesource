build:
	mvn install

deploy:
	echo ${MAVEN_GPG_PRIVATE_KEY} > .deploy/private-key.gpg
	gpg --import .deploy/private-key.gpg
	mvn versions:set -DnewVersion=${TRAVIS_TAG}
	mvn clean deploy -DskipTests -P release --settings .deploy/settings.xml
