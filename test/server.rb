#!/usr/bin/env ruby
lib = File.expand_path('../../lib', __FILE__)
p File.expand_path('../../lib')
p __FILE__
p lib
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "bundler/setup"
require 'catmq'

Catmq::Server.new.run
