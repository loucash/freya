var $ = require("jquery");
var c3 = require("c3");

var API_BASE = "http://localhost:8666/api/v1/metrics/ns";

var req_start = null;
var chart = null;

$(document).ready(function() {

    init_chart = function() {
        if (chart) {
            chart.destroy();
        }
        chart = c3.generate({
            data: { x: "x", columns: [ [], [] ] },
            axis: { x: { type: "timeseries", tick: { format: "%Y-%m-%d" } } },
            subchart: { show: true },
            zoom: { enabled: true }
        });
    }

    $(document).ajaxStart(function() {
        req_start = new Date().getTime();
        $('#status').fadeIn();
    });


    $(document).ajaxStop(function() {
        $('#status').fadeOut();
    });

    $.ajaxSetup({
        headers: {
            "Accept":"application/json",
            "Content-type":"application/json"
        },
        method: "GET",
        dataType: "json"
    });

    var update_time = function(call) {
        var log = $("#log").html();
        $("#log").html(call + ": "+ (new Date().getTime() - req_start) + "ms\n"+log);
    }

    var feed_namespaces = function() {
        $.ajax({
            url: API_BASE,
            success: function(result, ts, jqxhr) {
                update_time("get namespaces");
                $("#namespace").empty();
                $.each(result, function(idx) {
                    var opt = $("<option>").html(result[idx]).attr("value", result[idx]);
                    $("#namespace").append(opt);
                });
                feed_metrics($("#namespace").first().val());
            }
        });
    }

    var feed_metrics = function(ns) {
        $.ajax({
            url: API_BASE + "/" + ns,
            success: function(result, ts, jqxhr) {
                update_time("get metrics");
                $("#metric").empty();
                $.each(result, function(idx) {
                    var opt = $("<option>").html(result[idx]).attr("value", result[idx]);
                    $("#metric").append(opt);
                });
            }
        });
    }

    var graph = function() {
        var ns = $("#namespace").val();
        var metric = $("#metric").val();
        var n = $("#n").val();
        var unit = $("#unit").val();
        var n2 = $("#n2").val();
        var sampling_unit = $("#sampling_unit").val();
        var aggregate = $("#aggregate").val();

        var agg = "";
        if (n2 && aggregate) {
            agg = "&aggregate="+aggregate+"&sampling="+n2+","+sampling_unit;
        }
        $.ajax({
            url: API_BASE + "/" + ns + "/" + metric + "?start_time=" + n + "," + unit +
                agg,
            success: function(result, ts, jqxhr) {
                update_time("query");
                var x = ["x"];
                var data = [ns + "/" + metric];
                if (result.length > 800) {
                    if (!confirm("You are about to plot over 800 data points. Are you sure?")) {
                        return false;
                    }
                }
                $.each(result, function(idx) {
                    ts = result[idx][0];
                    val = result[idx][1];
                    x.push(ts);
                    data.push(val);
                });
                if (!$("#stacked").prop("checked")) {
                    init_chart();
                }
                chart.load({columns: [ x, data ]});
            }
        });
    };

    var spam = function() {
        var spam_ns = $("#spam_ns").val();
        var spam_metric = $("#spam_metric").val();
        var metrics_count = $("#spam_number").val();
        var interval = $("#spam_interval").val();
        var now = Math.floor(Date.now());
        var points = [];
        for (i=0; i < metrics_count; i++) {
            points.push([now-(i*interval), Math.random()]);
        }
        $.ajax({
            method: "POST",
            url: API_BASE + "/" + spam_ns,
            data: JSON.stringify(
                    [{"name": spam_metric, "points": points}]),
            success: function(result, ts, jqxhr) {
                update_time("put metrics");
            }
        });
    };

    feed_namespaces();
    init_chart();

    $("#namespace").change(function() {
        var ns = $(this).val();
        feed_metrics(ns);
    });

    $("#spam-form").submit(function() {
        spam();
        return false;
    });

    $("#log-form").submit(function() {
        $('#log').empty();
        return false;
    });

    $("#clear-graph").on("click", function() {
        init_chart();
        return false;
    });

    $("#graph").on("click", function() {
        graph();
        return false;
    });

});
