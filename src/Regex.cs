using System;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Text.RegularExpressions;
using System.Collections;
using System.Collections.Generic;

public partial class UDF
{
	private static readonly Dictionary<string, Regex> RegexCache = new Dictionary<string, Regex>();

	private static Regex GetRegex( string pattern )
	{
		if ( !RegexCache.ContainsKey( pattern ) )
			lock ( RegexCache )
				if ( !RegexCache.ContainsKey( pattern ) )
					RegexCache[ pattern ] = new Regex( pattern, RegexOptions.Compiled );

		return RegexCache[ pattern ];
	}

	private static bool IsInvalid( params string[] values )
	{
		foreach ( var value in values )
			if ( string.IsNullOrEmpty( value ) )
				return true;

		return false;
	}

	[SqlFunction( IsDeterministic = true )]
	public static SqlString Match( String input, String pattern )
	{
		if ( IsInvalid( input, pattern ) )
			return new SqlString( null );

		var m = GetRegex( pattern ).Match( input );
		return new SqlString( m.Success ? m.Value : null );
	}


	[SqlFunction( IsDeterministic = true )]
	public static SqlBoolean IsMatch( String input, String pattern )
	{
		if ( IsInvalid( input, pattern ) )
			return new SqlBoolean( false );

		return GetRegex( pattern ).IsMatch( input );
	}

	[SqlFunction( IsDeterministic = true, IsPrecise = true )]
	public static SqlString GroupMatch( String input, String pattern, String group )
	{
		if ( IsInvalid( input, pattern, group ) )
			return new SqlString( null );
		
		var g = GetRegex( pattern ).Match( input ).Groups[ group ];
		return new SqlString( g.Success ? g.Value : null );
	}

	[SqlFunction( IsDeterministic = true, IsPrecise = true )]
	public static SqlString Replace( String input, String pattern, String replacement )
	{
		if ( IsInvalid( input, pattern, replacement ) )
			return new SqlString( null );
		
		return new SqlString( GetRegex( pattern ).Replace( input, replacement ) );
	}

	[SqlFunction( DataAccess = DataAccessKind.None, FillRowMethodName = "FillMatches",
		TableDefinition = "Position int, MatchText nvarchar(max)" )]
	public static IEnumerable Matches( String input, String pattern )
	{
		var matchCollection = new List<RegexMatch>();
		if ( IsInvalid( input, pattern ) )
			return matchCollection;

		foreach ( Match m in GetRegex( pattern ).Matches( input ) )
			matchCollection.Add( new RegexMatch( m.Index, m.Value ) );

		return matchCollection;
	}

	[SqlFunction( DataAccess = DataAccessKind.None, FillRowMethodName = "FillMatches",
		TableDefinition = "Position int, MatchText nvarchar(max)" )]
	public static IEnumerable Split( String input, String pattern )
	{
		var matchCollection = new List<RegexMatch>();
		if ( IsInvalid( input, pattern ) )
			return matchCollection;

		var splits = GetRegex( pattern ).Split( input );
		for ( int i = 0; i < splits.Length; i++ )
			matchCollection.Add( new RegexMatch( i, splits[ i ] ) );

		return matchCollection;
	}

	private class RegexMatch
	{
		public SqlInt32 Position { get; set; }
		public SqlString MatchText { get; set; }

		public RegexMatch( SqlInt32 position, SqlString match )
		{
			Position = position;
			MatchText = match;
		}
	}
}