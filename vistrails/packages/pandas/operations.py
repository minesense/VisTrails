###############################################################################
##
## Copyright (C) 2014-2015, New York University.
## Copyright (C) 2011-2014, NYU-Poly.
## Copyright (C) 2006-2011, University of Utah.
## All rights reserved.
## Contact: contact@vistrails.org
##
## This file is part of VisTrails.
##
## "Redistribution and use in source and binary forms, with or without
## modification, are permitted provided that the following conditions are met:
##
##  - Redistributions of source code must retain the above copyright notice,
##    this list of conditions and the following disclaimer.
##  - Redistributions in binary form must reproduce the above copyright
##    notice, this list of conditions and the following disclaimer in the
##    documentation and/or other materials provided with the distribution.
##  - Neither the name of the New York University nor the names of its
##    contributors may be used to endorse or promote products derived from
##    this software without specific prior written permission.
##
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
## AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
## THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
## PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
## CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
## EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
## PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
## OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
## WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
## OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
## ADVISED OF THE POSSIBILITY OF SUCH DAMAGE."
##
###############################################################################
from __future__ import division

import re
import pandas as pd
import os
import types

from vistrails.core.modules.vistrails_module import ModuleError, Module
from vistrails.core.modules.config import ModuleSettings, IPort, OPort

UNIT = 1./2

SHAPE_DF = [
	(1,1),

	(0,1),
	(0,0),
	(1,0),
	(1,2),

	(0,2),
	(0,1),
	(2,1),

	(2,2),
	(1,2),
	(1,1),

	(2,1),
	(2,0),
	(1,0),
]
SHAPE_DF = [(x[0]*UNIT, x[1]*UNIT) for x in SHAPE_DF]

class DataFrame(Module):
	_settings = ModuleSettings(abstract=True)

	def __init__(self):
		pass

	@staticmethod
	def validate(x):
		return isinstance(x, pd.DataFrame)

class DataFrameToClipboard(Module):
	_settings = ModuleSettings(abstract=False)

	_input_ports = [IPort(name="df", signature="org.vistrails.vistrails.pandas:DataFrame", shape=SHAPE_DF),]
	_output_ports = []

	def __init__(self):
		Module.__init__(self)

	def compute(self):
		df = self.get_input("df")
		if (df is not None):
			df.to_clipboard()
		else:
			raise ModuleError(self, 'DataFrame = None')

class DataFrameToCSV(Module):
	_settings = ModuleSettings(abstract=False)

	_input_ports = [
		IPort(name='df', signature='org.vistrails.vistrails.pandas:DataFrame', shape=SHAPE_DF),
		IPort(name='output_path', signature='basic:OutputPath'), # basic:File is only for files that already exist
		IPort(name='overwrite_if_exists', signature='basic:Boolean'),
	]
	_output_ports = []

	def __init__(self):
		Module.__init__(self)

	def compute(self):
		pathOb = self.get_input('output_path')
		path = pathOb.name

		overwrite = self.get_input('overwrite_if_exists')

		if (os.path.exists(path) and not overwrite):
			raise ModuleError(self, 'Output path already exists, and user has required that files not be overwritten, aborting CSV export.')
		else:
			df = self.get_input('df')
			if (df is not None):
				df.to_csv(path)
				print 'DataFrameToCSV: wrote CSV'
			else:
				raise ModuleError(self, 'No data found. DataFrame = None')

class LoadFile(Module):
	_settings = ModuleSettings(abstract=False)

	_input_ports = [
		IPort(name='input_path', signature='basic:String'), # basic:File is only for files that already exist
	]
	_output_ports = [
		OPort(name='df', signature='org.vistrails.vistrails.pandas:DataFrame', shape=SHAPE_DF),
	]

	def compute(self):
		fpath = self.get_input('input_path')

		ext = os.path.splitext(fpath)[1]
		if (ext == '.pkl'):
			df = pd.read_pickle(fpath)
		elif (ext == '.xlsx'):
			df = pd.read_excel(fpath)
		elif (ext == '.csv'):
			df = pd.read_csv(fpath)
		else:
			raise ValueError(Fore.RED+'Filename extension not recognized (%s); expecting pkl, xlsx, or csv.' % fpath +Fore.RESET)

		self.set_output('df', df)

class DataFrameToVistrailsTable(Module):
	""" Converts a Pandas DataFrame to VisTrails Table, for viewing in VisTrails Spreadsheet. """

	_settings = ModuleSettings(abstract=False)

	_input_ports = [
		IPort(name='df', signature='org.vistrails.vistrails.pandas:DataFrame', shape=SHAPE_DF)
	]
	_output_ports = [
		OPort(name='table', signature='org.vistrails.vistrails.tabledata:Table'),
	]

	def compute(self):
		df = self.get_input('df')

		n, d = df.shape

		def get_column(self, colIdx):
			return self.df[self.df.columns[colIdx]].tolist()

		# create an instance of made-up class, which will imitate the behavior of the real TableData
		# just enough to get the required functionality to work
		out = type("TableDataImitator", (object,), {})
		out.columns = d
		out.rows = n
		out.df = df
		out.get_column = types.MethodType(get_column, out)
		out.names = df.columns.tolist()

		self.set_output('table', out)


_modules = [DataFrame, DataFrameToClipboard, DataFrameToCSV, LoadFile, DataFrameToVistrailsTable]


###############################################################################

# import unittest
# from vistrails.tests.utils import execute, intercept_result
# from .identifiers import identifier


# class TestJoin(unittest.TestCase):
#     ...