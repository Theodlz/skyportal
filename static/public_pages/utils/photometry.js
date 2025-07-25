const MJD_OFFSET = 40587;
const daysToSec = (days) => days * 24 * 60 * 60;

function photometryPlot(
  photometry_data,
  div_id,
  filters_used_mapper,
  isMobile,
  t0,
  displayXAxisSinceT0,
  displayInLog,
) {
  function getBaseLayout() {
    return {
      zeroline: false,
      automargin: true,
      showline: true,
      titlefont: { size: 18 },
      tickfont: { size: 14 },
      ticklen: 12,
      ticks: "outside",
      nticks: 8,
      minor: {
        ticks: "outside",
        ticklen: 6,
        tickcolor: "black",
      },
    };
  }

  function getLayoutGraphPart() {
    let title;
    if (t0 && displayXAxisSinceT0 && displayInLog) {
      title = "T - T0 (s)";
    } else if (t0 && displayXAxisSinceT0 && !displayInLog) {
      title = "T - T0 (days)";
    } else {
      title = "Days Ago";
    }
    return {
      autosize: true,
      xaxis: {
        title: {
          text: title,
        },
        overlaying: "x",
        side: "bottom",
        tickformat: ".6~f",
        autorange: t0 && displayXAxisSinceT0 ? true : "reversed",
        ...(t0 &&
          displayXAxisSinceT0 &&
          displayInLog && {
            type: "log",
            showexponent: "all",
            exponentformat: "power",
            tickformat: false,
          }),
        ...getBaseLayout(),
      },
      yaxis: {
        title: {
          text: "AB Mag",
        },
        autorange: "reversed",
        ...getBaseLayout(),
      },
      margin: {
        b: 75,
        l: 70,
        pad: 0,
        r: 30,
        t: 80,
      },
      shapes: [
        {
          type: "rect",
          xref: "paper",
          yref: "paper",
          x0: 0,
          y0: 0,
          x1: 1,
          y1: 1,
          line: {
            color: "black",
            width: 1,
          },
        },
      ],
      showlegend: true,
      hovermode: "closest",
    };
  }

  function getLayoutLegendPart() {
    return {
      legend: {
        font: { size: 14 },
        tracegroupgap: 0,
        orientation: isMobile ? "h" : "v",
        y: isMobile ? -0.5 : 1,
        x: isMobile ? 0 : 1,
      },
    };
  }

  function getConfig() {
    return {
      responsive: true,
      displaylogo: false,
      showAxisDragHandles: false,
      modeBarButtonsToRemove: [
        "autoScale2d",
        "resetScale2d",
        "select2d",
        "lasso2d",
        "toggleSpikelines",
        "hoverClosestCartesian",
        "hoverCompareCartesian",
      ],
      modeBarButtonsToAdd: [
        {
          name: "Reset",
          icon: Plotly.Icons.home, // eslint-disable-line no-undef
          click: (plotElement) => {
            // eslint-disable-next-line no-undef
            Plotly.relayout(plotElement, getLayoutGraphPart());
          },
        },
      ],
    };
  }

  function getHoverText(point) {
    let text = `MJD: ${point.mjd.toFixed(6)}<br>`;
    if (t0) {
      text += `T - T0: ${daysToSec(point.mjd - t0).toLocaleString("en-US", {
        maximumFractionDigits: 0,
      })}<br>`;
    }
    if (point.mag !== null) {
      text += `Mag: ${point.mag.toFixed(4)}<br>`;
    }
    if (point.magerr !== null) {
      text += `Magerr: ${point.magerr.toFixed(4)}<br>`;
    }
    if (point.limiting_mag !== null) {
      text += `Limiting Mag: ${point.limiting_mag.toFixed(4)}<br>`;
    }
    return `${text}Filter: ${point.filter}<br>Instrument: ${point.instrument_name}`;
  }

  function getUpdatedGroupedPhotometry(photometry) {
    const now = new Date().getTime() / 86400000 + MJD_OFFSET;
    return photometry.reduce((acc, point) => {
      const key = `${point.instrument_name}/${point.filter}${
        point.origin !== "None" ? `/${point.origin}` : ""
      }`;
      if (!acc[key]) {
        acc[key] = [];
      }
      if (!t0 || !displayXAxisSinceT0) {
        point.days_ago = now - point.mjd;
      } else if (displayInLog) {
        point.sec_since_t0 = daysToSec(point.mjd - t0);
      } else {
        point.days_since_t0 = point.mjd - t0;
      }
      acc[key].push(point);
      return acc;
    }, {});
  }

  function getTrace(data, isDetection, key, color) {
    const rgba = (rgb, alpha) =>
      `rgba(${rgb[0]},${rgb[1]},${rgb[2]}, ${alpha})`;
    const dataType = isDetection ? "detections" : "upperLimits";
    return {
      dataType,
      x: data.map(
        (point) => point.days_ago || point.sec_since_t0 || point.days_since_t0,
      ),
      y: data.map((point) => (isDetection ? point.mag : point.limiting_mag)),
      ...(isDetection
        ? {
            error_y: {
              type: "data",
              array: data.map((point) => point.magerr),
              visible: true,
              color: rgba(color, 0.5),
              width: 1,
              thickness: 2,
            },
          }
        : {}),
      text: data.map((point) => getHoverText(point)),
      mode: "markers",
      type: "scatter",
      name: key + (isDetection ? "" : " (UL)"),
      legendgroup: key + dataType,
      marker: {
        line: {
          width: 1,
          color: rgba(color, 1),
        },
        color: isDetection ? rgba(color, 0.3) : rgba(color, 0.1),
        size: isMobile ? 6 : 9,
        symbol: isDetection ? "circle" : "triangle-down",
      },
      hoverlabel: {
        bgcolor: "white",
        font: { size: 14 },
        align: "left",
      },
      hovertemplate: "%{text}<extra></extra>",
    };
  }

  const photometry_tab = JSON.parse(photometry_data);
  const mapper = JSON.parse(filters_used_mapper);
  const plotData = [];
  const groupedPhotometry = getUpdatedGroupedPhotometry(photometry_tab);
  Object.keys(groupedPhotometry).forEach((key) => {
    const photometry = groupedPhotometry[key];
    const color = mapper[photometry[0].filter] || [0, 0, 0];
    const { detections, upperLimits } = photometry.reduce(
      (acc, point) => {
        if (point.mag !== null) {
          acc.detections.push(point);
        } else {
          acc.upperLimits.push(point);
        }
        return acc;
      },
      { detections: [], upperLimits: [] },
    );
    const detectionsTrace = getTrace(detections, true, key, color, isMobile);
    const upperLimitsTrace = getTrace(upperLimits, false, key, color, isMobile);
    plotData.push(detectionsTrace, upperLimitsTrace);
  });

  // eslint-disable-next-line no-undef
  Plotly.newPlot(
    document.getElementById(div_id),
    plotData,
    { ...getLayoutGraphPart(), ...getLayoutLegendPart() },
    getConfig(),
  );
}
