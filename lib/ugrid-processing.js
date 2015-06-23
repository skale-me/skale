'use strict';

var RDD = require('./ugrid-transformation.js');

module.exports.UgridJob = function(grid, app, param) {
	this.id = param.jobId;
	this.node = param.node;
	this.stage = [];
	this.scnt = 0;
	this.app = app;
	this.action = new RDD[param.actionData.fun](grid, app, this, param.actionData);

	var job = this;

	// Spawn nodes
	for (var i in this.node)
		this.node[i] = new RDD[this.node[i].type](grid, app, this, this.node[i]);

	// Spawn stages
	for (var i = 0; i < param.stageData.length; i++)
		this.stage[i] = new Stage({sid: i, stageData: param.stageData[i], isLastStage: i == (param.stageData.length - 1)});

	// Run consiste à effectuer un treewalk sur le graph
	// il faut remplacer les références ancetres et fils avec les références de noeuds afin de réutiliser le code de treewalk
	// existant
	this.run = function() {this.stage[0].run();};

	// La logique des stages doit etre portées par les noeuds de shuffle et le noeud d'action
	// quand une source termine son run, elle notifie son stageNode de la fin du lineage
	// quand tous les lineages du stageNode sont terminés, le shuffle est effectué par le shuffle node
	// quand tous les shuffles sont reçus au niveau du stageNode, ce noeud invoque sa propre méthode
	// run afin de poursuivre le calcul

	// Phase 1, le run du job consiste en l'appel du runSource de l'ensemble des feuilles du graphes
	// connus à l'aide la structure de données stageData utilisée actuellement
	// Phase 2, on effectura un treewalk au niveau de chaque worker pour déterminer les feuilles
	// et lancer les runSource des feuilles détectées

	// un stage node doit donc :
	// - connaitre le nombre de lineage qu'il attend
	// - connaitre sa liste de noeud à exécuter
	// - posséder un logique lui permettant d'etre notifié des fins de lineages

	function Stage(param) {
		this.lineages = param.stageData.lineages;

		this.cnt = 0;									// Number of finished lineages
		this.target_cnt = this.lineages.length;			// Number of lineages
		this.locked = false;							// Because of an asynchonous lineage
		this.nShuffle = 0;								// Number of shuffle received
		this.sid = param.sid;							// stage index
		this.next_target_cnt = this.lineages.length;	// next iteration lineage target count
		this.shuffleNum = param.stageData.shuffleNum;	// shuffle node number

		var self = this;

		this.run = function () {
			for (var i = 0; i < this.lineages.length; i++) {
				try {
					job.node[this.lineages[i][0]].runSource(function() {
						if ((++self.cnt < self.target_cnt) || self.locked) return;
						if (!param.isLastStage) {
							job.node[self.shuffleNum].tx_shuffle();
							if (self.nShuffle == app.worker.length) job.stage[++job.scnt].run();
						} else job.action.sendResult();
					});
				} catch (err) {
					console.error(err.stack);
					throw new Error("Lineage error, " + err);
				}
			}
		};
	}
};