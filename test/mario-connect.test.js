'use strict';

var expect = require('chai').expect,
    MarioConnect = require('..'),
    seneca = require('seneca')();

    var chai = require("chai");
    var chaiAsPromised = require("chai-as-promised");
    chai.use(chaiAsPromised);

     describe('#Mario-Connect', function() {
        it('connects successfully', function() {
            var mc = new MarioConnect(seneca);
            return mc
                .start()
                .then(function(){
                    //mc.status();
                    expect(mc.listeners).to.be.an('Array');
                    expect(mc.publishers).to.be.an('Array');
                    expect(mc.listeners.length + mc.publishers.length).to.be.above(0);
                });
        });

        it('connects successfully to specific service', function() {
            var mc = new MarioConnect(seneca, 'Participant', 'Create');
            return mc
                .start()
                .then(function(){
                    //mc.status();
                    expect(mc.listeners).to.be.an('Array');
                    expect(mc.publishers).to.be.an('Array');
                    expect(mc.listeners.length).to.equal(1);
                });
        });

        it('should raise an error if no metadata found', function() {
            var mc = new MarioConnect(seneca, 'AnteGeia', 'Twra');
            return expect(mc.start()).to.eventually.be.rejectedWith(Error);
        });
    });
