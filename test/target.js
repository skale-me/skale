// stdout Write example
process.stdout.write('hello world on standard output\n');
// console.log('hello world')

// stderr Write example
process.stderr.write('hello world on error output\n');
// console.error('error message')

var i = 0;
while(i < 1000000000) {
	i++;
}