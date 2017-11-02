all:

# When editing docs, run 'make edit' then browse http://localhost:8000
edit:
	docker run --rm -it -p 8000:8000 -v $${PWD%/*}:/docs squidfunk/mkdocs-material

# Maintainer only, to deploy static doc website on github pages
deploy:
	docker run --rm -it -v $$HOME/.ssh:/root/.ssh -v $${PWD%/*}:/docs squidfunk/mkdocs-material gh-deploy
