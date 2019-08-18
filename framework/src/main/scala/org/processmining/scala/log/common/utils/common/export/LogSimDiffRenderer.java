package org.processmining.scala.log.common.utils.common.export;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.StandardXYBarPainter;
import org.jfree.chart.renderer.xy.XYBarRenderer;
import org.jfree.data.statistics.HistogramDataset;
import org.jfree.data.statistics.HistogramType;

import java.awt.*;
import java.io.FileOutputStream;
import java.io.IOException;

public final class LogSimDiffRenderer {


    public static boolean writeHistogramm(final DescriptiveStatistics ds, final String filename, final Color color, final int bins, final int height) throws IOException {
        if (ds.getN() > 0) {
            final HistogramDataset dataset = new HistogramDataset();
            dataset.setType(HistogramType.FREQUENCY);
            dataset.addSeries("Hist", ds.getValues(), bins);
            final String plotTitle = "";
            final String xAxis = "";
            final String yAxis = "";
//        String xAxis = "Diff, ms";
//        String yAxis = "Number";
            final PlotOrientation orientation = PlotOrientation.VERTICAL;

            final JFreeChart chart = ChartFactory.createHistogram(plotTitle, xAxis, yAxis,
                    dataset, orientation, false, false, false);

            final XYPlot plot = (XYPlot) chart.getPlot();
            final Font font = new Font("Arial", Font.PLAIN, 7);
            {
                ValueAxis axis = plot.getDomainAxis();
                //axis.setLabelFont(font);
                axis.setTickLabelFont(font);

            }
            {
                ValueAxis axis = plot.getRangeAxis();
                //axis.setLabelFont(font);
                axis.setTickLabelFont(font);
            }
            //axis.setLowerBound(0);
            final XYBarRenderer r = (XYBarRenderer) plot.getRenderer();
            r.setBarPainter(new StandardXYBarPainter());
            r.setSeriesPaint(0, color);
            r.setShadowVisible(false);
            chart.setBackgroundPaint(Color.white);
            //r.setBaseItemLabelFont(font);
            r.setBaseLegendTextFont(font);

            ChartUtilities.writeChartAsPNG(new FileOutputStream(filename), chart, bins, height);
            return true;
        } else {
            return false;
        }
    }

}
