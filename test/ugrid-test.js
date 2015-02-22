function groupByKey(v_in) {
	var v = JSON.parse(JSON.stringify(v_in));

	var keys = [];
	for (var i = 0; i < v.length; i++)
		if (keys.indexOf(v[i][0]) == -1)
			keys.push(v[i][0]);

	var res = [];
	for (var i = 0; i < keys.length; i++) 
		res.push([keys[i], []]);
	for (var i = 0; i < v.length; i++) {
		var idx = keys.indexOf(v[i][0]);
		res[idx][1].push(v[i][1]);
	}
	return res;
}

module.exports.groupByKey = groupByKey;