build:
	mvn install

deploy:
	openssl aes-256-cbc -K ${encrypted_bbcf2f683b6c_key} -iv ${encrypted_bbcf2f683b6c_iv} -in .deploy/private-key.gpg.enc -out .deploy/private-key.gpg -d
	gpg --import .deploy/private-key.gpg
	mvn versions:set -DnewVersion=${TRAVIS_TAG}
	mvn clean deploy -DskipTests -P release --settings .deploy/settings.xml

docs:
	mvn install -DskipTests
	mvn javadoc:javadoc -P release
	git config --global user.email "travis@travis-ci.com"
	git config --global user.name "travis-ci"

	# Decrypt the file containing the private key
	# .deploy/docs_deploy_key.enc is the travis-encrypted private key (public key added as a deploy key to simplesourcing/simplesourcing.github.io)
	openssl aes-256-cbc -K ${$encrypted_bbcf2f683b6c_key} -iv ${$encrypted_bbcf2f683b6c_iv} -in .deploy/docs_deploy_key.enc -out ${HOME}/.ssh/docs_deploy_key -d

	# Enable SSH authentication
	chmod 600 "${HOME}/.ssh/docs_deploy_key" \
	         && printf "%s\n" \
	              "Host github.com" \
	              "  IdentityFile ${HOME}/.ssh/docs_deploy_key" \
	              "  LogLevel ERROR" >> ~/.ssh/config

	rm -rf simplesourcing.github.io
	git clone --quiet git@github.com:simplesourcing/simplesourcing.github.io.git > /dev/null
	cd simplesourcing.github.io && \
	    rm -rf apidocs && \
	    cp -Rf ../target/site/apidocs apidocs && \
	    git add . && \
	    git commit -m "Lastest javadoc on successful travis deploy ${TRAVIS_BUILD_NUMBER} auto-pushed to simplesourcing.github.io"
	rm -rf simplesourcing.github.io
