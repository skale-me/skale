#!/usr/local/bin/node --harmony
'use strict';

/*
	CSV file contains stripe events gathered from stripe API,
	This program extract subscription periods associated to each customer using
	the following format for each line:
	{cid: string, subscriptions: {start: timestamp, end: timestamp}
*/

var parseCSV = require('papaparse').parse;
var fs = require('fs');
var readline = require('readline');

var rl = readline.createInterface({
	input: fs.createReadStream('/Users/cedricartigue/Desktop/stripe_events_bak.csv', {encoding: 'utf8'})
});

var first_line = true, fields, customerCnt = 0, lineCount = 0;;
var customers = {};

rl.on('line', function(line) {
	if (first_line) {
		first_line = false;
		fields = line.split(',').map(JSON.parse);
		return;
	};
	rl.pause();
	var data = parseCSV(line, {dynamicTyping: true});
	var json = {};
	for (var i = 0; i < fields.length; i++)
		json[fields[i]] = data.data[0][i];
	json.value = JSON.parse(json.value);

	switch (json.type) {
		case 'customer.subscription.created':
			// console.log(JSON.stringify(json))
			// console.log(JSON.stringify({
			// 	cid: json.stripe_customer_id,
			// 	period: {
			// 		start: json.value.data.object.current_period_start,
			// 		end: json.value.data.object.current_period_end
			// 	}
			// }));
			break;
		case 'customer.subscription.updated': 	// pas bon pour l'extract car intègre les refus de paiement également
			// if ((json.plan_interval != 'year') && (json.plan_interval != 'month'))
			// 	console.log(json)

			// console.log(JSON.stringify({
			// 	cid: json.stripe_customer_id,
			// 	period: {
			// 		start: json.value.data.object.current_period_start,
			// 		end: json.value.data.object.current_period_end
			// 	}
			// }));
			break;
		case 'invoice.payment_succeeded': 		// Meilleure méthode pour le calcul du churn rate réel
			console.log(JSON.stringify(json))
			// try {
			// 	var data = json.value.data.object;
			// 	var cid = json.stripe_customer_id;
			// 	console.log(JSON.stringify({cid: cid, period: data.lines.subscriptions[0].period}));
			// } catch (e) {
			// 	console.log('\nStructure des données différente: ');
			// 	console.log(JSON.stringify(json));
			// }
			break;
	}

	rl.resume();
});

// rl.on('close', function() {
// 	console.log(customers)	
// })

// stripe events 
// [ 'invoice.payment_succeeded',
//   'customer.created',
//   'invoice.created',
//   'customer.subscription.created',
//   'customer.subscription.deleted',
//   'customer.subscription.updated',
//   'charge.failed',
//   'charge.succeeded',
//   'invoice.updated',
//   'invoice.payment_failed',
//   'customer.updated',
//   'charge.refunded',
//   'transfer.created',
//   'charge.disputed',
//   'customer.discount.created',
//   'invoiceitem.created',
//   'invoiceitem.deleted',
//   'customer.deleted',
//   'transfer.updated',
//   'coupon.created',
//   'invoiceitem.updated',
//   'charge.dispute.created',
//   'charge.dispute.closed',
//   'account.application.deauthorized',
//   'customer.discount.updated',
//   'transfer.paid',
//   'balance.available',
//   'plan.created',
//   'plan.deleted',
//   'customer.card.created',
//   'customer.card.deleted',
//   'plan.updated',
//   'customer.card.updated',
//   'customer.discount.deleted',
//   'customer.subscription.trial_will_end' ]

