# Contributing

Interested in contributing to skale-engine? We'd love
your help. Skale-engine is an open source project, built one
contribution at a time by users just like you.

## Where to get help or report a problem

* If you have a question about using skale-engine, start a discussion
  on [gitter] or on [google group]
* If you think you have found a bug within skale-engine, open an
  [issue].  Do not forget to check that it doesn't already exist
  in our [issue database]
* If you want to learn more about skale internals, architecture and
  how to extend skale-engine, see the
  [Skale Hacker's Guide](doc/skale-hackers-guide.md)
* If you have a suggestion for improvement or a new feature, create
  a [pull request] so it can be discussed and reviewed by the
  community and project committers. Even the project committers
  submit their code this way.

## Submitting a pull request

* Create your own [fork] on github, then checkout your fork
* Write your code in your local copy. It's good practice to create
  a branch for each new issue you work on, although not compulsory
* Your code must follow existing coding style, and tests must pass.
  To check coding style, run `npm run lint`. The [coding style] of skale
  is the same as in core NodeJS.
  To run the tests, first run `npm install`, then `npm test`
* If the tests pass, you can commit changes to your fork and then
  create a pull request from there. Reference any relevant issue by
  including its number in the message, e.g. #123

## Documentation

The [documentation guidelines] from Google provide a good reference
for writing consistent and good technical documents, in particular
[API documentation rules].

Note: skale documentation was started before knowing this standard,
thus is not yet fully compliant! Fixes are welcome here.

## Coding rules

In addition to applying the already mentioned [coding style],
the following conventions should be applied as well:

* Use `const` instead of `var` for declarations, whenever possible
* Use `let` instead of `var` if reference must be reassigned
* Use array or object destructuring to set variables from array or
  object: `let [a, b] = [1, 2, 3]`
* Use arrow functions in callbacks, where applicable: `map`, `reduce`,
  `aggregate`, etc

Note: the code base is not yet fully compliant to these rules. Contributions
are welcome here.

[coding style]: https://github.com/felixge/node-style-guide
[gitter]: https://gitter.im/skale-me/skale-engine
[google group]: https://groups.google.com/forum/#!forum/skale
[issue database]: https://github.com/skale-me/skale-engine/issues
[issue]: https://github.com/skale-me/skale-engine/issues/new
[pull request]: #submitting-a-pull-request
[fork]: https://github.com/skale-me/skale-engine
[documentation guidelines]: https://developers.google.com/style/
[API documentation rules]: https://developers.google.com/style/api-reference-comments
