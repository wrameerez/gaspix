#!/usr/bin/env ruby
require 'rubygems'
require 'active_record'
#require 'composite_primary_keys'

#
# pixelLoader
#


# ActiveRecord::Base.logger = Logger.new(STDERR)
# ActiveRecord::Base.colorize_logging = false

#
# connect to the database
#
ActiveRecord::Base.establish_connection(:adapter => 'postgresql',
                                        :host => 'localhost',
                                        :username => 'ramiro',
                                        :password => 'sickboy',
                                        :database => 'ip261');



#
# Create base domain model classes for database interaction
#
class Trajectory < ActiveRecord::Base
	set_table_name      	"trajectory"
	has_many                :AtmospherePoints
end

class AtmospherePoint < ActiveRecord::Base
	set_table_name      	"atmospherepoint"
	belongs_to          	:Trajectory, :foreign_key => "trajectory"
	has_many            	:PointData, :class_name => "PointValue"
end

class PointDataType < ActiveRecord::Base
	set_table_name      	"pointdatatype"
	has_many 	    	    :PointData, :class_name => "PointValue"
end

class PointValue < ActiveRecord::Base
	set_table_name		    "pointdata"
#	set_primary_keys    	:point, :pointtype
	belongs_to	    	    :pointtype, :class_name => "PointDataType", :foreign_key => "pointtype"
	belongs_to 	   	        :point, :class_name => "AtmospherePoint", :foreign_key => "point"
end



#
# PixelFileLoader will read the data file, acting in a polymorphic way with stream.
# Hence, you can open a file and read instances of PixelData like if you were reading a 
# text file.
#
class PixelFileReader

	#
	# Class Variables
	#
	@@headerSize = 16
	@@floatSize = 4
	@@separatorSize = 8
	@@attributes = []

	#
	# Load type structure from the database
	#
	attributeTypes = PointDataType.find(:all)
	attributeTypes.each do |attributeType|
		 @@attributes << attributeType.name
	end

	#
	# Class Methods
	#
	def PixelFileReader.columns
		return @@attributes.size + 3
	end

	def PixelFileReader.rowSize
		return (self.columns * @@floatSize) + @@separatorSize
	end

	def PixelFileReader.open( fileName )
		result = pixelReader = PixelFileReader.new( fileName )
		if block_given?
			begin
				result = yield pixelReader
			ensure
				pixelReader.close
			end
		end
		return result
	end

	#
	# Instance Methods
	#
	def initialize( fileName )
		@maxRows = (File.size( fileName ) - @@headerSize) / PixelFileReader.rowSize
		@file = File.open( fileName, "rb" )
		
		# For the first row, skip the header.
		@file.seek( @@headerSize )
		@row = 0
		
		# Enamble timestamp from file name
		start = fileName.index( '_' ) + 1
		@timeStamp = Time.utc( 
		            fileName[start      .. start + 3],      # Year
		            fileName[start + 4  .. start + 5],      # Month
		            fileName[start + 6  .. start + 7],      # Day
		            fileName[start + 8  .. start + 9],      # Hours
		            fileName[start + 10 .. start + 11],     # Minutes
		            fileName[start + 12 .. start + 13] )    # Seconds
		puts "\nProcessing: " + fileName
		puts "Pixels: " + @maxRows.to_s
		puts "UTC Moment: " + @timeStamp.to_s
	end
	
	def timeStamp
	    return @timeStamp
	end

	def getPixel
		@row += 1
		if @row < @maxRows
			return @file.read( PixelFileReader.rowSize).unpack('f' * PixelFileReader.columns )
		else
			self.close
			return nil
		end
	end

	def close
		@file.close
	end
	
	def goToRow( numRow )
	    @file.seek( @@headerSize + numRow * PixelFileReader.rowSize )
    end
	
end


#
# DBUploader
#
class DBUploader
	
	#
	# Class Methods
	#
    def DBUploader.for( fileName )
        return DBUploader.new( fileName )
    end
    
    def initialize( fileName )
        @pixelReader = PixelFileReader.open( fileName )
        @rowNumber = AtmospherePoint.count( :conditions => "moment = '#{ @pixelReader.timeStamp }'")
        @pixelReader.goToRow( @rowNumber )
        
        # Trap Ctrl-C
        @userInterrupt = false
        trap("INT") { @userInterrupt = true }
    end
    
    def hasTrajectories
        return Trajectory.count > 0
    end
    
    def loadRows
        # Decide if we need to be creating trajectories or not.
        # The condition to create trajectories is either because there are none
        # or because the first load was interrupted before it ended
        moments = AtmospherePoint.count( 'moment', :distinct => "moment" )
        createTrajectory = (!self.hasTrajectories or (moments == 1 and @rowNumber > 0))
        
        attributeTypes = PointDataType.find(:all)
        while row = @pixelReader.getPixel
            if @userInterrupt
                puts( "\nUser interrupt after inserting " + @rowNumber.to_s + " pixels.")
                exit
            end
            @rowNumber += 1
            ActiveRecord::Base.transaction do
                if @rowNumber % 1000 == 0
                    puts @rowNumber.to_s + " pixels inserted"
                end
                
                # Create new trajectory if required
    	        if createTrajectory
    	            Trajectory.create( :startdate => @pixelReader.timeStamp )
    	        end
    	        
    	        # Create pixel, using the proper trajectory, the timestamp in the file name
    	        # and the first 3 values (latitude, longitude and height)
    	        point = AtmospherePoint.create( 
    	            :trajectory => @rowNumber,
    	            :moment => @pixelReader.timeStamp,
    	            :longitude => row[0],
    	            :latitude => row[1],
    	            :height => row[2])
    	        
    	        # Create all the data values of each pixel    
    	        attributeTypes.each do |type|
    	            PointValue.create( 
    	                :pointtype => type, 
    	                :point => point, 
    	                :value => row[type.id + 2])
    	        end
    	    end
	    end
	end
end


#
# Main
#
if ARGV.size != 1
    puts "Usage: pixLoad <dataDir>"
    puts "dataDir: directory containing data files with names following this convention part2_19790201114500"
    exit
end
dir = Dir.new( ARGV[0] )
fileNames = dir.entries.sort.reject { |fileName| File.directory?(fileName) }
puts "Importing " + fileNames.size.to_s + " files."
fileNames.each do |fileName|
    DBUploader.for( File.join( dir.path, fileName )).loadRows
end

