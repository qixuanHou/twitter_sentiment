document.addEventListener("DOMContentLoaded", function(event) {

    var dflt = {
      min: 0,
      max: 100,
      donut: true,
      gaugeWidthScale: 0.6,
      counter: true,
      hideInnerShadow: true
    }

    var g1 = new JustGage({
      id: 'g1',
      value: 10 + {{streamData}},
      title: 'javascript call',
      defaults: dflt

    });

    setInterval(
      function()
      {
      url = window.location.href;
      userAcc = str.split(url, "/")[1];
      url = "/" + userAcc + "/get_publicopinion"
        $.getJSON(url, {}, function(number){
          console.log(number);
          g1.refresh(number)}
        )
      }, 1000

    );
  });
 

