"""Functional defations from common y vs datetime visualizations."""

import altair as alt


def line_chart(
    df,
    dt_col,
    y_col,
    color_col=None,
    x_title=None,
    y_title=None,
    legend_title=None,
    width=800,
    height=400,
    title=None,
    **line_kwargs,
) -> alt.Chart:
    """Create a line chart with Altair.

    Args:
        df (pd.DataFrame): DataFrame containing the data to plot.
        dt_col (str): Column name for the x-axis (datetime).
        y_col (str): Column name for the y-axis.
        color_col (str, optional): Column name for color encoding. Defaults to None.
        x_title (str, optional): Title for the x-axis. Defaults to None.
        y_title (str, optional): Title for the y-axis. Defaults to None.
        legend_title (str, optional): Title for the legend. Defaults to None.
        width (int, optional): Width of the chart. Defaults to 800.
        height (int, optional): Height of the chart. Defaults to 400.
        title (str, optional): Title of the chart. Defaults to None.
        **line_kwargs: Additional keyword arguments for the line mark.

    Returns:
        alt.Chart: An Altair chart object representing the line chart.
    """

    chart_base = alt.Chart(df).mark_line(**line_kwargs)

    if color_col is not None:
        chart = chart_base.encode(
            x=alt.X(dt_col, title=x_title, type="temporal"),
            y=alt.Y(y_col, title=y_title),
            color=alt.Color(color_col, title=legend_title),
        )
    else:
        chart = chart_base.encode(
            x=alt.X(dt_col, title=x_title, type="temporal"),
            y=alt.Y(y_col, title=y_title),
        )

    return chart.properties(
        width=width,
        height=height,
        title=title,
    )


def interval_chart(
    df,
    dt_col,
    y_col_lower,
    y_col_upper,
    color_col=None,
    x_title=None,
    y_title=None,
    legend_title=None,
    width=800,
    height=400,
    title=None,
    **errorband_kwargs,
) -> alt.Chart:
    """Create an interval chart with Altair.

    Args:
        df (pd.DataFrame): DataFrame containing the data to plot.
        dt_col (str): Column name for the x-axis (datetime).
        y_col_lower (str): Column name for the lower bound of the interval.
        y_col_upper (str): Column name for the upper bound of the interval.
        color_col (str, optional): Column name for color encoding. Defaults to None.
        x_title (str, optional): Title for the x-axis. Defaults to None.
        y_title (str, optional): Title for the y-axis. Defaults to None.
        legend_title (str, optional): Title for the legend. Defaults to None.
        width (int, optional): Width of the chart. Defaults to 800.
        height (int, optional): Height of the chart. Defaults to 400.
        title (str, optional): Title of the chart. Defaults to None.
        **errorband_kwargs: Additional keyword arguments for the error band mark.

    Returns:
        alt.Chart: An Altair chart object representing the interval chart.
    """
    chart_base = alt.Chart(df).mark_errorband(**errorband_kwargs)
    if color_col is not None:
        chart = chart_base.encode(
            x=alt.X(dt_col, title=x_title, type="temporal"),
            y=alt.Y(y_col_lower, title=y_title),
            y2=alt.Y2(y_col_upper, title=y_title),
            color=alt.Color(color_col, title=legend_title),
        )
    else:
        chart = chart_base.encode(
            x=alt.X(dt_col, title=x_title, type="temporal"),
            y=alt.Y(y_col_lower, title=y_title),
            y2=alt.Y2(y_col_upper, title=y_title),
        )

    return chart.properties(
        width=width,
        height=height,
        title=title,
    )


def point_chart(
    df,
    dt_col,
    y_col,
    color_col=None,
    x_title=None,
    y_title=None,
    legend_title=None,
    width=800,
    height=400,
    title=None,
    **point_kwargs,
) -> alt.Chart:
    """Create a line chart with Altair.

    Args:
        df (pd.DataFrame): DataFrame containing the data to plot.
        dt_col (str): Column name for the x-axis (datetime).
        y_col (str): Column name for the y-axis.
        color_col (str, optional): Column name for color encoding. Defaults to None.
        x_title (str, optional): Title for the x-axis. Defaults to None.
        y_title (str, optional): Title for the y-axis. Defaults to None.
        legend_title (str, optional): Title for the legend. Defaults to None.
        width (int, optional): Width of the chart. Defaults to 800.
        height (int, optional): Height of the chart. Defaults to 400.
        title (str, optional): Title of the chart. Defaults to None.
        **point_kwargs: Additional keyword arguments for the point mark.

    Returns:
        alt.Chart: An Altair chart object representing the point chart.
    """
    chart_base = alt.Chart(df).mark_point(**point_kwargs)
    if color_col is not None:
        chart = chart_base.encode(
            x=alt.X(dt_col, title=x_title, type="temporal"),
            y=alt.Y(y_col, title=y_title),
            color=alt.Color(color_col, title=legend_title),
        )
    else:
        chart = chart_base.encode(
            x=alt.X(dt_col, title=x_title, type="temporal"),
            y=alt.Y(y_col, title=y_title),
        )

    return chart.properties(
        width=width,
        height=height,
        title=title,
    )
