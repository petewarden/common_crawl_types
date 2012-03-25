#!/usr/bin/env ruby

require 'rubygems'
require 'aws-sdk'
require 'open3'

# monkeypatch to add requester-pays support
class AWS::S3::Request
  def canonicalized_headers
    headers["x-amz-request-payer"]='requester' # this is the magic...
    x_amz = headers.select{|name, value| name.to_s =~ /^x-amz-/i }
    x_amz = x_amz.collect{|name, value| [name.downcase, value] }
    x_amz = x_amz.sort_by{|name, value| name }
    x_amz = x_amz.collect{|name, value| "#{name}:#{value}" }.join("\n")
    x_amz == '' ? nil : x_amz
  end
end

# Inline this so we don't have to copy a file while bootstrapping
class ArcFile

  include Enumerable

  def initialize( input_stream )
    @handle=input_stream
  end

  def each
    return self.to_enum unless block_given?
    begin
      # See http://www.archive.org/web/researcher/ArcFileFormat.php
      # for information about the ARC format once it is decompressed
      main_header=@handle.readline.strip

      main_header_body=@handle.read( Integer(main_header.split.last) )
      loop do
        begin
          record_header=@handle.readline.strip

          record_body=@handle.read( Integer(record_header.split.last) )
          unless (byte=@handle.read(1))=="\n"
            raise ArgumentError, "#{self.class}: Corrupt ARCfile? Expected \\n as record terminator, got #{byte}"
          end
          yield [record_header, record_body]
        rescue EOFError
          break nil
        end
      end
    rescue
      raise "#{self.class}: Error processing - #{$!}"
    end
  end

end

CHUNKSIZE=1024*1024

# All these warnings will end up in the EMR stderr logs.
warn "Starting up, using #{CHUNKSIZE/1024}KB chunks for download."

# CHANGEME! - You'll need to put your own Amazon keys in here
s3=AWS::S3.new(
  :access_key_id=>'',
  :secret_access_key=>''
)

ARGF.each_line {|line|
  warn "Starting work on #{line.chomp}"
  # expect a line like this:
  # s3://commoncrawl-crawl-002/2010/09/24/9/1285380159663_9.arc.gz
  proto,unused,bucket_name,*rest=line.chomp.split File::SEPARATOR
  raise ArgumentError, "#{__FILE__}: Unknown S3 Protocol #{proto}" unless proto=~/^s3/
  object_name=File.join rest

  size=Integer( s3.buckets[bucket_name].objects[object_name].content_length )
  warn "Reading from #{bucket_name.inspect}, #{object_name.inspect}, size #{size}"
  ranges=(0..size).each_slice( CHUNKSIZE ).map {|ary| (ary.first..ary.last)}

  # Ruby GzipReader is unable to unzip these files, but unix gunzip can
  # Also means we don't need to eat much RAM, because everything is streaming.
  Open3.popen3( 'gunzip -c' ) {|sin,sout,serr,thr|

    # Create an ArcFile instance which will receive gunzip's stdout
    arcfile=ArcFile.new sout

    Thread.new do
      # Download chunks in the background and pipe them into gunzip
      # as we receive them
      ranges.each {|target_range|
        retry_count=5
        begin
          chunk=s3.buckets[bucket_name].objects[object_name].read( :range => target_range )
        rescue
          raise $! if (retry_count-=1)<0
          warn "Error (#{$!}) downloading #{target_range}, retrying."
          sleep 1 and retry
        end
        sin.write chunk
        Thread.pass
      }
      sin.close # which will send an EOF to the ArcFile
    end

    # Now we have a lazy ArcFile that we can treat as an Enumerable.
    arcfile.each {|header, body|
      # mimetype and URL extension (but don't keep ? params to php urls etc)
      puts( "#{header.split[3]}".ljust(25) << "#{File.extname( header.split.first ).split('?').first}".ljust(15) )
    }
  }
}
