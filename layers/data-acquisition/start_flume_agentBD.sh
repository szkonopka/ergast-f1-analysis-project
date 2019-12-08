#!/bin/sh

flume-ng agent --conf-file flumeAgentBDConfig.properties --name agentBD -Dflume.root.logger=INFO,console