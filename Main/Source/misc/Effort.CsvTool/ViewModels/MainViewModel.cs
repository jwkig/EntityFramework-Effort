// ----------------------------------------------------------------------------------
// <copyright file="MainViewModel.cs" company="Effort Team">
//     Copyright (C) 2011-2013 Effort Team
//
//     Permission is hereby granted, free of charge, to any person obtaining a copy
//     of this software and associated documentation files (the "Software"), to deal
//     in the Software without restriction, including without limitation the rights
//     to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//     copies of the Software, and to permit persons to whom the Software is
//     furnished to do so, subject to the following conditions:
//
//     The above copyright notice and this permission notice shall be included in
//     all copies or substantial portions of the Software.
//
//     THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//     IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//     FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//     AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//     LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//     OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
//     THE SOFTWARE.
// </copyright>
// ----------------------------------------------------------------------------------

using System.Net.Mime;
using System.Reflection;
using System.Text;
using System.Windows;

namespace Effort.CsvTool.ViewModels
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.ComponentModel;
    using System.Data;
    using System.Data.Common;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Windows.Input;

    public class MainViewModel : ViewModelBase
    {
        private ObservableCollection<ProviderViewModel> _providers;
        private ICommand _exportCommand;

        private ProviderViewModel _selectedProvider;
        private DbConnectionStringBuilder _connectionStringBuilder;
        private int _reportProgress;
        private string _statusText;

        private BackgroundWorker _worker;

        public MainViewModel()
        {
            _providers = new ObservableCollection<ProviderViewModel>();

            foreach (DataRow item in DbProviderFactories.GetFactoryClasses().Rows)
            {
                _providers.Add(new ProviderViewModel((string)item["Name"], (string)item["AssemblyQualifiedName"]));
            }

            _worker = new BackgroundWorker();

            _worker.WorkerReportsProgress = true;
            _worker.DoWork += new DoWorkEventHandler(worker_DoWork);
            _worker.ProgressChanged += new ProgressChangedEventHandler(worker_ProgressChanged);
            _worker.RunWorkerCompleted += new RunWorkerCompletedEventHandler(worker_RunWorkerCompleted);

            SelectedProvider = _providers.FirstOrDefault();
            _exportCommand = new RelayCommand(Export);

            ExportPath = Environment.GetFolderPath(Environment.SpecialFolder.Desktop);
        }


        public ObservableCollection<ProviderViewModel> Providers
        {
            get 
            { 
                return _providers; 
            }
        }

        public ProviderViewModel SelectedProvider
        {
            get
            {
                return _selectedProvider;
            }
            set
            {
                _selectedProvider = value;
                SetupProperties();
            }
        }

        public DbConnectionStringBuilder ConnectionStringBuilder 
        { 
            get 
            { 
                return _connectionStringBuilder; 
            }
            set
            {
                _connectionStringBuilder = value;
                NotifyChanged(nameof(ConnectionStringBuilder));
            }
        }

        public string ExportPath { get; set; }

        public int ReportProgress 
        {
            get { return _reportProgress; }
            set 
            {
                _reportProgress = value;
                NotifyChanged(nameof(ReportProgress));
            }
        }

        public string StatusText
        {
            get => _statusText;
            set
            {
                _statusText = value;
                NotifyChanged(nameof(StatusText));
            }
        }


        public ICommand ExportCommand
        {
            get { return _exportCommand; }
        }


        private void SetupProperties()
        {
            var provider = SelectedProvider;

            if (provider == null)
            {
                return;
            }

            var factory = provider.GetProviderFactory();
            var connectionStringBuilder = factory.CreateConnectionStringBuilder();

            ConnectionStringBuilder = connectionStringBuilder;
        }

        private void Export(object arg)
        {
            var provider = SelectedProvider;

            if (provider == null)
            {
                return;
            }

            var factory = provider.GetProviderFactory();

            var connection = factory.CreateConnection();

            connection.ConnectionString = ConnectionStringBuilder.ConnectionString;

            var args = new WorkerArgs()
            {
                Connection = connection,
                ExportPath = ExportPath
            };

            _worker.RunWorkerAsync(args);
        }


        private class WorkerArgs
        {
            public DbConnection Connection { get; set; }
            public string ExportPath { get; set; }
        }

        private class WorkerResults
        {
            public StringBuilder Log { get; set; }
        }

        private class WorkerProgress
        {
            public string TableName { get; set; }
            public long CurrentRow { get; set; }
            public long TotalRows { get; set; }
        }

        private void worker_DoWork(object sender, DoWorkEventArgs e)
        {
            var args = e.Argument as WorkerArgs;
            var con = args.Connection;
            var dir = new DirectoryInfo(args.ExportPath);
            var orConType = Type.GetType("System.Data.OracleClient.OracleConnection, System.Data.OracleClient, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089");

            var conType = con.GetType();
            var isOracle = conType == orConType;
            var userId = string.Empty;
            if (isOracle)
            {
                var conOptions = con.ConnectionString.Split(';').Select(pair => pair.Split('='))
                    .ToDictionary(p => p.First().ToUpper(), p => p.Last());
                var userIdKey = "USER ID";
                if(conOptions.ContainsKey(userIdKey))
                    userId = conOptions[userIdKey];
            }

            var log = new StringBuilder();

            var results = new WorkerResults() {Log = log};
            e.Result = results;

            var progress = new WorkerProgress();

            using(new CultureScope(CultureInfo.InvariantCulture))
            using (con)
            {
                con.Open();

                var schema = con.GetSchema("Tables");

                var tables = new Dictionary<string, List<string>>();
                //List<string> tables = new List<string>();

                foreach (DataRow item in schema.Rows)
                {
                    if (isOracle)
                    {
                        if (item[2].Equals("User"))
                        {
                            var schemaName = item[0] as string;
                            if(!schemaName.Equals(userId, StringComparison.OrdinalIgnoreCase))
                                continue;
                            var name = item[1] as string;
                            if (!tables.ContainsKey(schemaName))
                            {
                                tables.Add(schemaName, new List<string>());
                            }
                            tables[schemaName].Add(name);
                        }
                    }
                    else
                    {
                        if (item[3].Equals("BASE TABLE"))
                        {
                            var schemaName = item[1] as string;
                            var name = item[2] as string;

                            if (!tables.ContainsKey(schemaName))
                            {
                                tables.Add(schemaName, new List<string>());
                            }

                            tables[schemaName].Add(name);
                        }
                    }
                }
                foreach (var schemaName in tables.Keys)
                {
                    for (var j = 0; j < tables[schemaName].Count; j++)
                    {
                        var rowCount = 0;
                        var totalCount = 0;
                        var name = tables[schemaName][j];
                        progress.TableName = $"{schemaName}.{name}";
                        progress.CurrentRow = 0;
                        using (var cmd = con.CreateCommand())
                        {
                            cmd.CommandText =
                                $"SELECT COUNT(*) FROM {(isOracle ? "\"" : "[")}{schemaName}{(isOracle ? "\"" : "]")}.{(isOracle ? "\"" : "[")}{name}{(isOracle ? "\"" : "]")}";
                            cmd.CommandType = CommandType.Text;
                            using (var reader = cmd.ExecuteReader())
                            {
                                if (reader.Read())
                                {
                                    totalCount = reader.GetInt32(0);
                                }
                            }
                        }

                        progress.TotalRows = totalCount;
                        using (var cmd = con.CreateCommand())
                        {
                            cmd.CommandText = $"SELECT * FROM {(isOracle ? "\"":"[")}{schemaName}{(isOracle ? "\"":"]")}.{(isOracle ? "\"":"[")}{name}{(isOracle ? "\"":"]")}";
                            cmd.CommandType = CommandType.Text;

							var file = new FileInfo(Path.Combine(dir.FullName, $"{schemaName}.{name}.csv"));

                            if (!dir.Exists)
                            {
                                dir.Create();
                            }

                            using (var reader = cmd.ExecuteReader())
                            using (var sw = new StreamWriter(file.Open(FileMode.Create, FileAccess.Write, FileShare.None)))
                            {
                                var fieldCount = reader.FieldCount;

                                var fieldNames = new string[fieldCount];
                                var serializers = new Func<object, string>[fieldCount];
                                var typeNeedQuote = new bool[fieldCount];
                                var sTable = reader.GetSchemaTable();

                                for (var i = 0; i < fieldCount; i++)
                                {
                                    fieldNames[i] = reader.GetName(i);

                                    var fieldType = reader.GetFieldType(i);
                                    var fieldTypeName = reader.GetDataTypeName(i);
                                    var fieldPsType = reader.GetProviderSpecificFieldType(i);

                                    if (fieldType == typeof(Byte[]))
                                    {
                                        serializers[i] = BinarySerializer;
                                        typeNeedQuote[i] = false;
                                    }
                                    else if (fieldType == typeof(string) | (fieldType==typeof(Guid)))
                                    {
                                        serializers[i] = DefaultSerializer;
                                        typeNeedQuote[i] = true;
                                    }
                                    else if (fieldType == typeof(bool))
                                    {
                                        serializers[i] = BooleanSerializer;
                                        typeNeedQuote[i] = false;
                                    }
                                    else if (fieldType == typeof(decimal) &&
                                             fieldTypeName.Equals("number", StringComparison.OrdinalIgnoreCase))
                                    {
                                        serializers[i] = DefaultSerializer;
                                        typeNeedQuote[i] = false;

                                        // посмотрим в табличку со схемой и оценим размер поля
                                        var row = sTable.AsEnumerable()
                                            .FirstOrDefault(r => r.Field<string>("ColumnName") == fieldNames[i]);
                                        if (row != null)
                                        {
                                            if (row.Field<short>("NumericPrecision") == 1)
                                            {
                                                serializers[i] = BooleanSerializer;
                                                typeNeedQuote[i] = false;
                                            }
                                        }
                                    }
                                    else
                                    {
                                        // Default serializer
                                        serializers[i] = DefaultSerializer;
                                        typeNeedQuote[i] = false;
                                    }

                                }

                                sw.WriteLine(string.Join(",", fieldNames));

                                var values = new object[fieldCount];
                                var serializedValues = new string[fieldCount];
                                var addQuote = new bool[fieldCount];

                                while (reader.Read())
                                {
                                    try
                                    {
                                        rowCount++;
                                        progress.CurrentRow = rowCount;
                                        reader.GetValues(values);

                                        for (var i = 0; i < fieldCount; i++)
                                        {
                                            var value = values[i];

                                            // Check if null
                                            if (value == null || value is DBNull)
                                            {
                                                addQuote[i] = false;
                                                serializedValues[i] = "";
                                            }
                                            else
                                            {
                                                addQuote[i] = typeNeedQuote[i];
                                                serializedValues[i] = serializers[i](value);
                                            }
                                        }

                                        {
                                            var i = 0;
                                            for (; i < fieldCount - 1; i++)
                                            {
                                                AppendField(sw, serializedValues[i], addQuote[i]);

                                                sw.Write(',');
                                            }

                                            AppendField(sw, serializedValues[i], addQuote[i]);
                                            sw.WriteLine();
                                        }
                                        _worker.ReportProgress((int)((j + 1) * 100.0 / tables[schemaName].Count), progress);
                                    }
                                    catch (Exception exception)
                                    {
                                        log.AppendLine($"{DateTime.Now.ToString()} - Table '{schemaName}.{name}', row {rowCount},  Error ({exception.Message})");
                                    }

                                }
                                // DataReader is finished
                            }

                            // Remove Empty file
                            if (rowCount == 0)
                            {
                                file.Delete();
                            }
                            // Command is finished
                        }

                        _worker.ReportProgress((int)((j + 1) * 100.0 / tables[schemaName].Count));

                        // Table is finished
                    }
                }
                
                // All table finished
            }
            // Connection is closed
        }

        private static void AppendField(StreamWriter sw, string value, bool addQuote)
        {
            var append = ConvertToCsv(value);

            if (addQuote)
            {
                sw.Write("\"{0}\"", append);
            }
            else
            {
                sw.Write(append);
            }
        }

        private static string ConvertToCsv(string input)
        {
            return input
                .Replace("\"", "\"\"")
                .Replace("\\", "\\\\")
                .Replace("\n", "\\n")
                .Replace("\r", "\\r");
        }

        private static string DefaultSerializer(object input)
        {
            return input.ToString();
        }

        private static string BinarySerializer(object input)
        {
            var bin = input as byte[];

            return Convert.ToBase64String(bin);
        }

        private static string BooleanSerializer(object input)
        {
            var result = string.Empty;
            try
            {
                if (input != null)
                {
                    result = "false";
                    result = Convert.ToBoolean(input).ToString();
                }
            }
            catch // тут просто глотаем исключения, и возвращаем пустую строку,
            {
            }
            return result;
        }



        private void worker_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            var progress = e.UserState as WorkerProgress;
            if (progress != null)
            {
                StatusText = $"Working '{progress.TableName}' row {progress.CurrentRow} of {progress.TotalRows}";
            }
            ReportProgress = e.ProgressPercentage;
        }

        private void worker_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            ReportProgress = 0;
            if (e.Error != null)
            {
                MessageBox.Show(e.Error.ToString(), "Ошибка");
            }

            var result = e.Result as WorkerResults;
            Console.Write(result.Log.ToString());
        }
    }




}
