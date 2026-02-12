namespace OpenPlot.Core.TimeSeries;

public readonly record struct Point(DateTime Ts, double Val);
public readonly record struct MinMaxPoint(DateTime Ts, double Val);
